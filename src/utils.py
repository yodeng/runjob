import os
import io
import re
import sys
import pdb
import time
import types
import socket
import signal
import psutil
import logging
import argparse
import textwrap
import threading
import traceback
import contextlib
import pkg_resources

from string import Template
from datetime import datetime
from fractions import Fraction
from threading import Thread, Lock
from collections import Counter, deque, OrderedDict
from functools import total_ordering, wraps, partial
from subprocess import check_output, call, Popen, PIPE
from os.path import dirname, basename, isfile, isdir, exists, normpath, realpath, abspath, splitext, join, expanduser

from .loger import *
from .config import which

PY3 = sys.version_info.major == 3

if not PY3:
    from Queue import Queue, Empty
    from collections.abc import MutableSet
else:
    from queue import Queue, Empty
    from collections import MutableSet


QSUB_JOB_ID_DECODER = re.compile("Your job (\d+) \(.+?\) has been submitted")

TIMEDELTA_REGEX = re.compile(r'^((?P<weeks>[\.\d]+?)w)? *'
                             r'^((?P<days>[\.\d]+?)d)? *'
                             r'((?P<hours>[\.\d]+?)h)? *'
                             r'((?P<minutes>[\.\d]+?)m)? *'
                             r'((?P<seconds>[\.\d]+?)s?)?$', re.IGNORECASE)

MULTIPLIERS = {
    # 'years': 60 * 60 * 24 * 365,
    # 'months': 60 * 60 * 24 * 30,
    'weeks': 60 * 60 * 24 * 7,
    'days': 60 * 60 * 24,
    'hours': 60 * 60,
    'minutes': 60,
    'seconds': 1
}


STYLE = {
    'fore': Formatter.f_color_map,
    'back': Formatter.b_color_map,
    'mode': Formatter.mode_map,
    'default': {'end': 0},
}


class JobFailedError(Exception):

    def __init__(self, msg="", jobs=None):
        self.msg = msg
        self.failed_jobs = jobs and [j for j in jobs if j.is_fail]

    def __str__(self):
        if self.msg:
            return self.msg
        fj = self.failed_jobs
        fj_names = [j.jobname for j in fj]
        fj_logs = [j.logfile for j in fj]
        s = "{} jobs {} failed, please check in logs: {}".format(
            len(fj), fj_names, fj_logs)
        return style(s, fore="red", mode="bold")


class RunJobException(Exception):

    def __init__(self, msg=""):
        self.msg = msg

    def __str__(self):
        return style(self.msg, fore="red", mode="bold")

    __repr__ = __str__


class QsubError(RunJobException):
    pass


class JobError(RunJobException):
    pass


class JobOrderError(RunJobException):
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

    def _exit(self):
        self.obj.safe_exit()

    def signal_handler(self, signum, frame):
        self.obj.signaled = True
        self._exit()
        # os._exit(signum)  # Force Exit
        sys.exit(signum)    # SystemExit Exception

    def signal_handler_us(self, signum, frame):
        self.obj.signaled = True
        self._exit()
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
        self._stop_event = threading.Event()

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

    def stop(self):
        if not self.stopped():
            self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class DummyFile(object):
    def write(self, x):
        pass


def style(string, mode='', fore='', back=''):
    mode = '%s' % STYLE["mode"].get(mode, "")
    fore = '%s' % STYLE['fore'].get(fore, "")
    back = '%s' % STYLE['back'].get(back, "")
    style = ';'.join([s for s in [mode, fore, back] if s])
    style = '\033[%sm' % style if style else ''
    end = '\033[%sm' % STYLE['default']['end'] if style else ''
    return '%s%s%s' % (style, string, end)


REQUIRED = style("(required)", fore="green", mode="bold")


class mute(object):

    def __init__(self, func):
        wraps(func)(self)

    def __call__(self, *args, **kwargs):  # wrapper function
        if sys.version_info >= (3, 5):
            with open(os.devnull, 'w') as devnull:
                with contextlib.redirect_stdout(devnull):
                    return self.__wrapped__(*args, **kwargs)
        else:
            sys.stdout = DummyFile()
            try:
                return self.__wrapped__(*args, **kwargs)
            finally:
                sys.stdout = sys.__stdout__

    def __get__(self, instance, cls):  # wrapper instance method
        if instance is None:
            return self
        return types.MethodType(self, instance)


class MaxRetryError(RunJobException):
    pass


def retry(func=None, *, max_num=3, delay=5, callback=None):
    if func is None:
        return partial(retry, max_num=max_num, delay=delay, callback=callback)
    elif not callable(func):
        raise TypeError("Not a callable. Did you use a non-keyword argument?")
    log = logging.getLogger(__package__)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try_num = 0
        while try_num < max_num+1:
            try_num += 1
            try:
                if try_num > 1:
                    log.warning("retry %s", try_num-1)
                res = func(*args, **kwargs)
            except Exception as e:
                if try_num > 1:
                    log.error("retry %s error, %s", try_num-1, e)
                else:
                    log.error(e)
                if try_num <= max_num:
                    time.sleep(delay)
                continue
            else:
                break
        else:
            raise MaxRetryError("max retry %s error" % max_num)
        if callback:
            return callback(res)
        return res
    return wrapper


def getlog(logfile=None, level="info", name=__package__):
    logger = logging.getLogger(name)
    if level.lower() == "info":
        logger.setLevel(logging.INFO)
    elif level.lower() == "debug":
        logger.setLevel(logging.DEBUG)
    if logfile is None:
        if logger.hasHandlers():
            return logger
        h = logging.StreamHandler(sys.stdout)
    else:
        h = logging.FileHandler(logfile, mode='w')
    h.setFormatter(Formatter())
    logger.addHandler(h)
    return logger


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


def seconds2human(s):
    m, s = divmod(s, 60)
    h, m = divmod(int(m), 60)
    return "{:d}:{:02d}:{:04.2f}".format(h, m, s)


def human2seconds(time_str):
    "valid strings: '8h', '2d 8h 5m 2s', '2m4.3s'"
    parts = TIMEDELTA_REGEX.match(str(time_str))
    time_params = {name: float(param)
                   for name, param in parts.groupdict().items() if param}
    return int(sum(MULTIPLIERS[k]*v for k, v in time_params.items()))


def mkdir(*path):
    for p in path:
        if not isdir(p):
            try:
                os.makedirs(p)
            except:
                pass


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


def is_entry_cmd():
    prog = abspath(realpath(sys.argv[0]))
    return basename(prog) in \
        list(pkg_resources.get_entry_map(__package__).values())[0].keys() \
        and join(sys.prefix, "bin", basename(prog)) == prog


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
        hostname = splitext(socket.gethostname())[0]
        try:
            with os.popen("qconf -ss") as fi:
                for line in fi:
                    if line.strip() == hostname or splitext(line.strip())[0] == hostname:
                        return True
        except:
            return False
    return False


class AppDirs(object):
    """Convenience wrapper for getting application dirs."""

    def __init__(self, appname=None, version=None):
        self.appname = appname
        self.version = version

    @property
    def user_data_dir(self):
        path = os.getenv('XDG_DATA_HOME', os.path.expanduser("~/.local/share"))
        return self.__user_dir(path)

    @property
    def site_data_dir(self):
        path = os.getenv('XDG_DATA_DIRS',
                         os.pathsep.join(['/usr/local/share', '/usr/share']))
        return self.__site_dir(path)

    @property
    def site_config_dir(self):
        path = os.getenv('XDG_CONFIG_DIRS', '/etc/xdg')
        return self.__site_dir(path)

    @property
    def user_config_dir(self):
        path = os.getenv('XDG_CONFIG_HOME', os.path.expanduser("~/.config"))
        return self.__user_dir(path)

    @property
    def user_cache_dir(self):
        path = os.getenv('XDG_CACHE_HOME', os.path.expanduser('~/.cache'))
        return self.__user_dir(path)

    @property
    def user_state_dir(self):
        path = os.getenv('XDG_STATE_HOME',
                         os.path.expanduser("~/.local/state"))
        return self.__user_dir(path)

    @property
    def user_log_dir(self):
        return os.path.join(self.user_cache_dir, "log")

    def __user_dir(self, path):
        if self.appname:
            path = os.path.join(path, self.appname)
        if self.appname and self.version:
            path = os.path.join(path, self.version)
        return path

    def __site_dir(self, path, multipath=False):
        pathlist = [os.path.expanduser(x.rstrip(os.sep))
                    for x in path.split(os.pathsep)]
        appname = self.appname
        version = self.version
        if appname:
            if version:
                appname = os.path.join(appname, version)
            pathlist = [os.sep.join([x, appname]) for x in pathlist]
        if multipath:
            path = os.pathsep.join(pathlist)
        else:
            path = pathlist[0]
        return path


def user_config_dir(app=__package__, version=""):
    app = AppDirs(app, version)
    return app.user_config_dir


class RateLimiter(object):
    """Provides rate limiting for an operation with a configurable number of
    requests for a time period.
    """

    def __init__(self, max_calls, period=1.0, callback=None):
        """Initialize a RateLimiter object which enforces as much as max_calls
        operations on period (eventually floating) number of seconds.
        """
        if period <= 0:
            raise ValueError('Rate limiting period should be > 0')
        if max_calls <= 0:
            raise ValueError('Rate limiting number of calls should be > 0')

        # We're using a deque to store the last execution timestamps, not for
        # its maxlen attribute, but to allow constant time front removal.
        self.calls = deque()

        self.period = period
        self.max_calls = max_calls
        self.callback = callback
        self._lock = Lock()

        # Lock to protect creation of self._alock
        self._init_lock = Lock()

    def __call__(self, f):
        """The __call__ function allows the RateLimiter object to be used as a
        regular function decorator.
        """
        @wraps(f)
        def wrapped(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return wrapped

    def __enter__(self):
        with self._lock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            if len(self.calls) >= self.max_calls:
                until = now() + self.period - self._timespan
                if self.callback:
                    t = Thread(target=self.callback, args=(until,))
                    t.daemon = True
                    t.start()
                sleeptime = until - now()
                if sleeptime > 0:
                    time.sleep(sleeptime)
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            # Store the last operation timestamp.
            self.calls.append(now())

            # Pop the timestamp list front (ie: the older calls) until the sum goes
            # back below the period. This is our 'sliding period' window.
            while self._timespan >= self.period:
                self.calls.popleft()

    @property
    def _timespan(self):
        return self.calls[-1] - self.calls[0]


class CmdTemplate(Template):
    delimiter = "$"
    idpattern = "(?a:[_a-z][_\.a-z0-9]*)"


@total_ordering
class OrderedSet(OrderedDict, MutableSet):

    def __init__(self, items=""):
        for item in items:
            self.add(item)

    def update(self, *args):
        for s in args:
            for e in s:
                self.add(e)

    def add(self, elem):
        self[elem] = None

    def discard(self, elem):
        self.pop(elem, None)

    def __lt__(self, other):
        return all(e in other for e in self) and self != other

    def __eq__(self, other):
        return all(i == j for i, j in zip(self, other))

    def __repr__(self):
        return 'OrderedSet([%s])' % (', '.join(map(repr, self.keys())))

    __str__ = __repr__
