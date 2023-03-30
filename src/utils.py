import os
import re
import sys
import pdb
import time
import signal
import psutil
import logging
import argparse
import threading

from threading import Thread
from collections import Counter
from subprocess import call, PIPE
from time import monotonic as now
from functools import total_ordering

from ratelimiter import RateLimiter

from .loger import *
from ._version import __version__

if sys.version_info[0] < 3:
    from Queue import Queue, Empty
else:
    from queue import Queue, Empty


RUNSTAT = " && echo [`date +'%F %T'`] SUCCESS || echo [`date +'%F %T'`] ERROR"

QSUB_JOB_ID_DECODER = re.compile("Your job (\d+) \(.+?\) has been submitted")


class QsubError(Exception):
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


def runsgeArgparser():
    parser = argparse.ArgumentParser(
        description="For multi-run your shell scripts localhost, qsub or BatchCompute.")
    parser.add_argument("-wd", "--workdir", type=str, help="work dir, default: %s" %
                        os.path.abspath(os.getcwd()), default=os.path.abspath(os.getcwd()), metavar="<workdir>")
    parser.add_argument("-N", "--jobname", type=str,
                        help="job name", metavar="<jobname>")
    parser.add_argument("-lg", "--logdir", type=str,
                        help='the output log dir, default: "%s/runjob_*_log_dir"' % os.getcwd(), metavar="<logdir>")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time. default: all in your job file, max 1000", metavar="<int>")
    parser.add_argument("-s", "--startline", type=int,
                        help="which line number(1-base) be used for the first job tesk. default: 1", metavar="<int>", default=1)
    parser.add_argument("-e", "--endline", type=int,
                        help="which line number (include) be used for the last job tesk. default: all in your job file", metavar="<int>")
    parser.add_argument("-g", "--groups", type=int, default=1,
                        help="groups number of lines to a new jobs", metavar="<int>")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='log debug info', default=False)
    parser.add_argument("-l", "--log", type=str,
                        help='append log info to file, sys.stdout by default', metavar="<file>")
    parser.add_argument('-r', '--resub', help="rebsub you job when error, 0 or minus means do not re-submit, 0 by default",
                        type=int, default=0, metavar="<int>")
    parser.add_argument('--init', help="initial command before all task if set, will be running in localhost",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--call-back', help="callback command if set, will be running in localhost",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost", "batchcompute"], help="the mode to submit your jobs, 'sge' by default")
    parser.add_argument('-ivs', '--resubivs', help="rebsub interval seconds, 2 by default",
                        type=int, default=2, metavar="<int>")
    parser.add_argument('--rate', help="rate limite for job status checking per second, 3 by default",
                        type=int, default=3, metavar="<int>")
    parser.add_argument('-ini', '--ini',
                        help="input configfile for configurations search.", metavar="<configfile>")
    parser.add_argument("-config", '--config',   action='store_true',
                        help="show configurations and exit.",  default=False)
    parser.add_argument("-f", "--force", default=False, action="store_true",
                        help="force to submit jobs ingore already successed jobs, skip by default")
    parser.add_argument("--local", default=False, action="store_true",
                        help="submit your jobs in localhost, same as '--mode local'")
    parser.add_argument("--strict", action="store_true", default=False,
                        help="use strict to run. Means if any errors occur, clean all jobs and exit programe. off by default")
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
    sge = parser.add_argument_group("sge arguments")
    sge.add_argument("-q", "--queue", type=str, help="the queue your job running, multi queue can be sepreated by whitespace, all access queue by default",
                     nargs="*", metavar="<queue>")
    sge.add_argument("-m", "--memory", type=int,
                     help="the memory used per command (GB), default: 1", default=1, metavar="<int>")
    sge.add_argument("-c", "--cpu", type=int,
                     help="the cpu numbers you job used, default: 1", default=1, metavar="<int>")
    batchcmp = parser.add_argument_group("batchcompute arguments")
    batchcmp.add_argument("-om", "--out-maping", type=str,
                          help='the oss output directory if your mode is "batchcompute", all output file will be mapping to you OSS://BUCKET-NAME. if not set, any output will be reserved', metavar="<dir>")
    batchcmp.add_argument('--access-key-id', type=str,
                          help="AccessKeyID while access oss", metavar="<str>")
    batchcmp.add_argument('--access-key-secret', type=str,
                          help="AccessKeySecret while access oss", metavar="<str>")
    batchcmp.add_argument('--regin', type=str, default="BEIJING", choices=['BEIJING', 'HANGZHOU', 'HUHEHAOTE', 'SHANGHAI',
                                                                           'ZHANGJIAKOU', 'CHENGDU', 'HONGKONG', 'QINGDAO', 'SHENZHEN'], help="batch compute regin, BEIJING by default")
    parser.add_argument("-j", "--jobfile", type=str,
                        help="the input jobfile", metavar="<jobfile>")
    return parser


def runjobArgparser():
    parser = argparse.ArgumentParser(
        description="For manger submit your jobs in a job file.")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time, default: 1000", default=1000, metavar="<int>")
    parser.add_argument("-j", "--jobfile", type=str, required=True,
                        help="the input jobfile", metavar="<jobfile>")
    parser.add_argument('-i', '--injname', help="job names you need to run, default: all jobnames of you job file",
                        nargs="*", type=str, metavar="<str>")
    parser.add_argument("-s", "--startline", type=int,
                        help="which line number(1-base) be used for the first job tesk. default: 1", metavar="<int>", default=1)
    parser.add_argument("-e", "--endline", type=int,
                        help="which line number (include) be used for the last job tesk. default: all in your job file", metavar="<int>")
    parser.add_argument('-r', '--resub', help="rebsub you job when error, 0 or minus means do not re-submit, 0 by default",
                        type=int, default=0, metavar="<int>")
    parser.add_argument('-ivs', '--resubivs', help="rebsub interval seconds, 2 by default",
                        type=int, default=2, metavar="<int>")
    parser.add_argument('--rate', help="rate limite for job status checking per second, 3 by default",
                        type=int, default=3, metavar="<int>")
    parser.add_argument("-m", '--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost"], help="the mode to submit your jobs, 'sge' by default, if no sge installed, always localhost.")
    parser.add_argument("--local", default=False, action="store_true",
                        help="submit your jobs in localhost, same as '--mode local'")
    parser.add_argument("--strict", action="store_true", default=False,
                        help="use strict to run. Means if any errors occur, clean all jobs and exit programe. off by default")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='log debug info', default=False)
    parser.add_argument("-l", "--log", type=str,
                        help='append log info to file, sys.stdout by default', metavar="<file>")
    parser.add_argument('-v', '--version',
                        action='version', version="%(prog)s v" + __version__)
    return parser.parse_args()


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
