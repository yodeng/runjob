import os
import sys
import logging
import threading

from collections import Counter
from subprocess import call, PIPE
from ratelimiter import RateLimiter

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
