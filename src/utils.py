import os
import sys
import pdb
import logging
import argparse
import threading

from collections import Counter
from subprocess import call, PIPE
from ratelimiter import RateLimiter

from .version import __version__

if sys.version_info[0] < 3:
    from Queue import Queue
else:
    from queue import Queue


RUNSTAT = " && echo [`date +'%F %T'`] SUCCESS || echo [`date +'%F %T'`] ERROR"


class QsubError(Exception):
    pass


class myQueue(object):

    def __init__(self, maxsize=0):
        self._content = set()
        self._queue = Queue(maxsize=maxsize)
        self.sm = threading.Semaphore(maxsize)
        self.lock = threading.Lock()

    @property
    def length(self):
        return self._queue.qsize()

    def put(self, v, **kwargs):
        self._queue.put(v, **kwargs)
        # self.sm.acquire()
        if v not in self._content:
            with self.lock:
                self._content.add(v)

    def get(self, v=None):
        self._queue.get()
        # self.sm.release()
        if v is None:
            with self.lock:
                o = self._content.pop()
                return o
        else:
            if v in self._content:
                with self.lock:
                    self._content.remove(v)
                    return v

    @property
    def queue(self):
        return self._content.copy()

    def isEmpty(self):
        return self._queue.empty()

    def isFull(self):
        return self._queue.full()


def Mylog(logfile=None, level="info", name=None):
    logger = logging.getLogger(name)
    if level.lower() == "info":
        logger.setLevel(logging.INFO)
        f = logging.Formatter(
            '[%(levelname)s %(asctime)s] %(message)s')
    elif level.lower() == "debug":
        logger.setLevel(logging.DEBUG)
        f = logging.Formatter(
            '[%(levelname)s %(threadName)s %(asctime)s %(funcName)s(%(lineno)d)] %(message)s')
    if logfile is None:
        h = logging.StreamHandler(sys.stdout)  # default: sys.stderr
    else:
        h = logging.FileHandler(logfile, mode='w')
    h.setFormatter(f)
    logger.addHandler(h)
    return logger


def cleanAll(clear=False, qjobs=None):
    if qjobs is None:
        return
    stillrunjob = qjobs.jobqueue.queue
    if clear:
        pid = os.getpid()
        gid = os.getpgid(pid)
        for jn in stillrunjob:
            if jn.status in ["error", "success"]:
                continue
            jn.status = "killed"
            qjobs.logger.info("job %s status killed", jn.name)
        sumJobs(qjobs)
        call('qdel "*_%d"' % os.getpid(),
             shell=True, stderr=PIPE, stdout=PIPE)
    else:
        for jn in stillrunjob:
            if jn.status in ["error", "success"]:
                continue
            jn.status += "-but-exit"
            qjobs.logger.info("job %s status %s", jn.name, jn.status)
        sumJobs(qjobs)


def sumJobs(qjobs):
    run_jobs = qjobs.jobs
    has_success_jobs = qjobs.has_success
    error_jobs = [j for j in run_jobs if j.status == "error"]
    success_jobs = [j for j in run_jobs if j.status == 'success']

    logger = logging.getLogger()
    status = "All tesks(total(%d), actual(%d), actual_success(%d), actual_error(%d)) in file (%s) finished" % (len(
        run_jobs) + len(has_success_jobs), len(run_jobs), len(success_jobs), len(error_jobs), os.path.abspath(qjobs.jfile))
    SUCCESS = True
    if len(success_jobs) == len(run_jobs):
        status += " successfully."
    else:
        status += ", but there are Unsuccessful tesks."
        SUCCESS = False
    logger.info(status)
    qjobs.writestates(os.path.join(qjobs.logdir, "job.status.txt"))
    logger.info(str(dict(Counter([j.status for j in run_jobs]))))
    return SUCCESS


def style(string, mode='', fore='', back=''):
    STYLE = {
        'fore': {'black': 30, 'red': 31, 'green': 32, 'yellow': 33, 'blue': 34, 'purple': 35, 'cyan': 36, 'white': 37},
        'back': {'black': 40, 'red': 41, 'green': 42, 'yellow': 43, 'blue': 44, 'purple': 45, 'cyan': 46, 'white': 47},
        'mode': {'mormal': 0, 'bold': 1, 'underline': 4, 'blink': 5, 'invert': 7, 'hide': 8},
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


def runsgeArgparser():
    parser = argparse.ArgumentParser(
        description="For multi-run your shell scripts localhost, qsub or BatchCompute.")
    parser.add_argument("-q", "--queue", type=str, help="the queue your job running, default: all.q",
                        default=["all.q", ], nargs="*", metavar="<queue>")
    parser.add_argument("-m", "--memory", type=int,
                        help="the memory used per command (GB), default: 1", default=1, metavar="<int>")
    parser.add_argument("-c", "--cpu", type=int,
                        help="the cpu numbers you job used, default: 1", default=1, metavar="<int>")
    parser.add_argument("-wd", "--workdir", type=str, help="work dir, default: %s" %
                        os.path.abspath(os.getcwd()), default=os.path.abspath(os.getcwd()), metavar="<workdir>")
    parser.add_argument("-N", "--jobname", type=str,
                        help="job name", metavar="<jobname>")
    parser.add_argument("-lg", "--logdir", type=str,
                        help='the output log dir, default: "%s/runjob_*_log_dir"' % os.getcwd(), metavar="<logdir>")
    parser.add_argument("-om", "--out-maping", type=str,
                        help='the oss output directory if your mode is "batchcompute", all output file will be mapping to you OSS://BUCKET-NAME. if not set, any output will be reserved', metavar="<dir>")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time. default: all in your job file", metavar="<int>")
    parser.add_argument("-s", "--startline", type=int,
                        help="which line number(0-base) be used for the first job tesk. default: 0", metavar="<int>", default=0)
    parser.add_argument("-e", "--endline", type=int,
                        help="which line number (include) be used for the last job tesk. default: all in your job file", metavar="<int>")
    parser.add_argument("-g", "--groups", type=int, default=1,
                        help="groups number of lines to a new jobs, will consider first line args as the group job args.", metavar="<int>")
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
    parser.add_argument('--access-key-id', type=str,
                        help="AccessKeyID while access oss", metavar="<str>")
    parser.add_argument('--access-key-secret', type=str,
                        help="AccessKeySecret while access oss", metavar="<str>")
    parser.add_argument('--regin', type=str, default="BEIJING", choices=['BEIJING', 'HANGZHOU', 'HUHEHAOTE', 'SHANGHAI',
                        'ZHANGJIAKOU', 'CHENGDU', 'HONGKONG', 'QINGDAO', 'SHENZHEN'], help="batch compute regin, BEIJING by default")
    parser.add_argument('-ivs', '--resubivs', help="rebsub interval seconds, 2 by default",
                        type=int, default=2, metavar="<int>")
    parser.add_argument('-ini', '--ini',
                        help="input configfile for configurations search.", metavar="<configfile>")
    parser.add_argument("-config", '--config',   action='store_true',
                        help="show configurations and exit.",  default=False)
    # parser.add_argument("--local", default=False, action="store_true",
    # help="submit your jobs in localhost instead of sge, if no sge installed, always localhost.")
    parser.add_argument("--strict", action="store_true", default=False,
                        help="use strict to run. Means if any errors occur, clean all jobs and exit programe. off by default")
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
    parser.add_argument("-j", "--jobfile", type=str,
                        help="the input jobfile", metavar="<jobfile>")
    return parser.parse_args()


def shellJobArgparser(arglist):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-q", "--queue", type=str, nargs="*")
    parser.add_argument("-m", "--memory", type=int)
    parser.add_argument("-c", "--cpu", type=int)
    parser.add_argument("-n", "--jobname", type=str)
    parser.add_argument("-om", "--out-maping", type=str)
    parser.add_argument('--mode', type=str)
    return parser.parse_known_args(arglist)[0]
