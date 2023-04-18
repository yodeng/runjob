import os
import re
import sys
import pdb
import time
import socket
import signal
import psutil
import logging
import argparse
import textwrap
import threading
import traceback
import pkg_resources

from threading import Thread
from datetime import datetime
from fractions import Fraction
from collections import Counter
from functools import total_ordering
from subprocess import check_output, call, Popen, PIPE

from ratelimiter import RateLimiter

from .loger import *
from .config import which
from ._version import __version__

if sys.version_info[0] < 3:
    from Queue import Queue, Empty
else:
    from queue import Queue, Empty


RUNSTAT = " && (echo [`date +'%F %T'`] SUCCESS) || (echo [`date +'%F %T'`] ERROR)"

QSUB_JOB_ID_DECODER = re.compile("Your job (\d+) \(.+?\) has been submitted")


class QsubError(Exception):
    pass


class JobFailedError(Exception):
    pass


class JobRuleError(Exception):
    pass


class JobOrderError(Exception):
    pass


class JobQueue(Queue):

    def _init(self, maxsize):
        self._queue = set()

    def _qsize(self):
        return len(self._queue)

    def _put(self, item):
        self._queue.add(item)

    def _get(self, name=None):
        if name is not None:
            if name in self._queue:
                self._queue.remove(name)
                return name
            else:
                raise KeyError(name)
        return self._queue.pop()

    def __contains__(self, item):
        return item in self._queue

    def __str__(self):
        return self._queue.__str__()

    __repr__ = __str__

    @property
    def length(self):
        return self.qsize()

    @property
    def queue(self):
        return sorted(self._queue)

    def puts(self, *items, **kw):
        for item in items:
            self.put(item, **kw)

    def get(self, name=None, block=True, timeout=None):
        with self.not_empty:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = now() + timeout
                while not self._qsize():
                    remaining = endtime - now()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get(name)
            self.not_full.notify()
            return item


class ParseSingal(Thread):

    def __init__(self, obj=None):
        super(ParseSingal, self).__init__()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGUSR1, self.signal_handler_us)
        self.daemon = True
        self.obj = obj

    def run(self):
        time.sleep(1)

    def clean_up(self):
        self.obj.clean_jobs()
        self.obj.sumstatus()

    def signal_handler(self, signum, frame):
        self.clean_up()
        # os._exit(signum)  # Force Exit
        sys.exit(signum)    # SystemExit Exception

    def signal_handler_us(self, signum, frame):
        self.clean_up()
        raise QsubError(self.obj.err_msg)


class RunThread(Thread):

    def __init__(self, func, *args):
        super(RunThread, self).__init__()
        self.args = args
        self.func = func
        self.exitcode = 0
        self.exception = None
        self.exc_traceback = ''
        self.daemon = True

    def run(self):
        try:
            self._run()
        except Exception as e:
            self.exitcode = 1
            self.exception = e
            self.exc_traceback = ''.join(
                traceback.format_exception(*sys.exc_info()))

    def _run(self):
        try:
            self.func(*(self.args))
        except Exception as e:
            raise e


def Mylog(logfile=None, level="info", name=None):
    logger = logging.getLogger(name)
    if level.lower() == "info":
        logger.setLevel(logging.INFO)
    elif level.lower() == "debug":
        logger.setLevel(logging.DEBUG)
    if logfile is None:
        h = logging.StreamHandler()
    else:
        h = logging.FileHandler(logfile, mode='w')
    h.setFormatter(Formatter())
    logger.addHandler(h)
    return logger


def style(string, mode='', fore='', back=''):
    STYLE = {
        'fore': Formatter.f_color_map,
        'back': Formatter.b_color_map,
        'mode': Formatter.mode_map,
        'default': {'end': 0},
    }
    mode = '%s' % STYLE["mode"].get(mode, "")
    fore = '%s' % STYLE['fore'].get(fore, "")
    back = '%s' % STYLE['back'].get(back, "")
    style = ';'.join([s for s in [mode, fore, back] if s])
    style = '\033[%sm' % style if style else ''
    end = '\033[%sm' % STYLE['default']['end'] if style else ''
    return '%s%s%s' % (style, string, end)


def get_job_state(state):
    s = state.lower() if state else state
    if s == 'running':
        return style(state, fore="cyan")
    if s == 'finished':
        return style(state, fore="green")
    elif s == 'waiting':
        return style(state, fore="white")
    elif s == 'failed':
        return style(state, fore="red")
    elif s == 'stopped':
        return style(state, fore="yellow")
    else:
        return style(state, fore="white")


def now():
    if hasattr(time, 'monotonic'):
        return time.monotonic()
    return time.time()


def mkdir(*path):
    for p in path:
        if not os.path.isdir(p):
            try:
                os.makedirs(p)
            except:
                pass


def is_entry_cmd():
    prog = os.path.abspath(os.path.realpath(sys.argv[0]))
    return os.path.basename(prog) in \
        list(pkg_resources.get_entry_map(__package__).values())[0].keys() \
        and os.path.join(sys.prefix, "bin", os.path.basename(prog)) == prog


def terminate_process(pid):
    try:
        pproc = psutil.Process(pid)
        for cproc in pproc.children(recursive=True):
            # cproc.terminate() # SIGTERM
            cproc.kill()  # SIGKILL
        # pproc.terminate()
        pproc.kill()
    except:
        pass


def call_cmd(cmd, verbose=False):
    shell = True
    if isinstance(cmd, list):
        shell = False
    if verbose:
        print(cmd)
        call(cmd, shell=shell, stdout=PIPE, stderr=PIPE)
    else:
        with open(os.devnull, "w") as fo:
            call(cmd, shell=shell, stdout=fo, stderr=fo)


def is_sge_submit():
    if os.getenv("SGE_ROOT") and which("qconf"):
        hostname = os.path.splitext(socket.gethostname())[0]
        try:
            with os.popen("qconf -ss") as fi:
                for line in fi:
                    ss = os.path.splitext(line.strip())[0]
                    if ss == hostname:
                        return True
        except:
            return False
    return False


def common_parser():
    p = argparse.ArgumentParser(add_help=False)
    common = p.add_argument_group("common arguments")
    common.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
    common.add_argument("-j", "--jobfile", type=str,
                        help="the input jobfile. " + style("(required)", fore="green", mode="bold"), metavar="<jobfile>")
    common.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time. (default: all of the jobfile, max 1000)", metavar="<int>")
    common.add_argument("-s", "--startline", type=int,
                        help="which line number(1-base) be used for the first job. (default: 1)", metavar="<int>", default=1)
    common.add_argument("-e", "--endline", type=int,
                        help="which line number (include) be used for the last job. (default: last line of the jobfile)", metavar="<int>")
    common.add_argument('-d', '--debug', action='store_true',
                        help='log debug info.', default=False)
    common.add_argument("-l", "--log", type=str,
                        help='append log info to file. (default: stdout)', metavar="<file>")
    common.add_argument('-r', '--resub', help="rebsub you job when error, 0 or minus means do not re-submit. (default: 0)",
                        type=int, default=0, metavar="<int>")
    common.add_argument('-ivs', '--resubivs', help="rebsub interval seconds. (default: 2)",
                        type=int, default=2, metavar="<int>")
    common.add_argument("-f", "--force", default=False, action="store_true",
                        help="force to submit jobs even if already successed.")
    common.add_argument("--local", default=False, action="store_true",
                        help="submit your jobs in localhost, same as '--mode local'.")
    common.add_argument("--strict", action="store_true", default=False,
                        help="use strict to run, means if any errors, clean all jobs and exit.")
    common.add_argument('--max-check', help="maximal number of job status checks per second, default is 3, fractions allowed. (default: 3)",
                        type=float, default=3, metavar="<float>")
    common.add_argument('--max-submit', help="maximal number of jobs submited per second, default is 30, fractions allowed. (default: 30)",
                        type=float, default=30, metavar="<float>")
    return p


def runsgeArgparser():
    parser = argparse.ArgumentParser(
        description="%(prog)s is a tool for managing parallel jobs from a specific shell scripts runing in localhost, SGE or BatchCompute.", parents=[common_parser()])
    parser.add_argument("-wd", "--workdir", type=str, help="work dir. (default: %s)" %
                        os.path.abspath(os.getcwd()), default=os.path.abspath(os.getcwd()), metavar="<workdir>")
    parser.add_argument("-N", "--jobname", type=str,
                        help="job name. (default: basename of the jobfile)", metavar="<jobname>")
    parser.add_argument("-lg", "--logdir", type=str,
                        help='the output log dir. (default: "%s/runjob_*_log_dir")' % os.getcwd(), metavar="<logdir>")
    parser.add_argument("-g", "--groups", type=int, default=1,
                        help="groups number of lines to a new jobs. (default: 1)", metavar="<int>")
    parser.add_argument('--init', help="command before all jobs, will be running in localhost.",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--call-back', help="command after all jobs finished, will be running in localhost.",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost", "batchcompute"], help="the mode to submit your jobs, if no sge installed, always localhost. (default: sge)")
    parser.add_argument('-ini', '--ini',
                        help="input configfile for configurations search.", metavar="<configfile>")
    parser.add_argument("-config", '--config',   action='store_true',
                        help="show configurations and exit.",  default=False)
    sge = parser.add_argument_group("sge arguments")
    sge.add_argument("-q", "--queue", type=str, help="the queue your job running, multi queue can be sepreated by whitespace. (default: all accessed queue)",
                     nargs="*", metavar="<queue>")
    sge.add_argument("-m", "--memory", type=int,
                     help="the memory used per command (GB). (default: 1)", default=1, metavar="<int>")
    sge.add_argument("-c", "--cpu", type=int,
                     help="the cpu numbers you job used. (default: 1)", default=1, metavar="<int>")
    batchcmp = parser.add_argument_group("batchcompute arguments")
    batchcmp.add_argument("-om", "--out-maping", type=str,
                          help='the oss output directory if your mode is "batchcompute", all output file will be mapping to you OSS://BUCKET-NAME. if not set, any output will be reserved.', metavar="<dir>")
    batchcmp.add_argument('--access-key-id', type=str,
                          help="AccessKeyID while access oss.", metavar="<str>")
    batchcmp.add_argument('--access-key-secret', type=str,
                          help="AccessKeySecret while access oss.", metavar="<str>")
    batchcmp.add_argument('--region', type=str, default="beijing", choices=['beijing', 'hangzhou', 'huhehaote', 'shanghai',
                                                                            'zhangjiakou', 'chengdu', 'hongkong', 'qingdao', 'shenzhen'], help="batch compute region. (default: beijing)")
    return parser


def runjobArgparser():
    parser = argparse.ArgumentParser(
        description="%(prog)s is a tool for managing parallel jobs from a job file running in localhost or SGE cluster.",  parents=[common_parser()])
    parser.add_argument('-i', '--injname', help="job names you need to run. (default: all job names of the jobfile)",
                        nargs="*", type=str, metavar="<str>")
    parser.add_argument("-m", '--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost"], help="the mode to submit your jobs, if no sge installed, always localhost. (default: sge)")
    return parser


def shellJobArgparser(arglist):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-q", "--queue", type=str, nargs="*")
    parser.add_argument("-m", "--memory", type=int)
    parser.add_argument("-c", "--cpu", type=int)
    parser.add_argument("-g", "--groups", type=int)
    parser.add_argument("-n", "--jobname", type=str)
    parser.add_argument("-om", "--out-maping", type=str)
    parser.add_argument("-wd", "--workdir", type=str)
    parser.add_argument('--mode', type=str)
    parser.add_argument("--local", default=False, action="store_true")
    return parser.parse_known_args(arglist)[0]
