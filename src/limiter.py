import os
import sys
import time
import signal
import resource

from collections import deque
from threading import Thread, Lock
from functools import wraps, partial


class RateLimiter(object):

    def __init__(self, max_calls, period=1.0, callback=None):
        if period <= 0:
            raise ValueError('Rate limiting period should be > 0')
        if max_calls <= 0:
            raise ValueError('Rate limiting number of calls should be > 0')

        self.calls = deque()
        self.period = period
        self.max_calls = max_calls
        self.callback = callback
        self._lock = Lock()

    def __call__(self, f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return wrapped

    def __enter__(self):
        with self._lock:
            if len(self.calls) >= self.max_calls:
                until = now(1) + self.period - self._timespan
                if self.callback:
                    t = Thread(target=self.callback, args=(until,))
                    t.daemon = True
                    t.start()
                sleeptime = until - now(1)
                if sleeptime > 0:
                    time.sleep(sleeptime)
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            self.calls.append(now(1))

            while self._timespan >= self.period:
                self.calls.popleft()

    @property
    def _timespan(self):
        return self.calls[-1] - self.calls[0]


def runtime_exceeded(signum, frame):
    raise RuntimeError


class ResourceLimiter(object):

    @staticmethod
    def max_runtime(seconds=sys.maxsize):
        def wrap(func):
            @wraps(func)
            def _run(*args, **kwargs):
                # if wraped instance method, add self as first argument
                signal.signal(signal.SIGALRM, runtime_exceeded)
                signal.alarm(seconds)
                res = func(*args, **kwargs)
                signal.alarm(0)
                return res
            return _run
        return wrap

    @staticmethod
    def max_memory(size=-1):
        def wrap(func):
            @wraps(func)
            def _run(*args, **kwargs):
                _, hard = resource.getrlimit(resource.RLIMIT_AS)
                resource.setrlimit(resource.RLIMIT_AS, (int(size), hard))
                return func(*args, **kwargs)
            return _run
        return wrap

    @staticmethod
    def max_nproc(nproc=4096):
        def wrap(func):
            @wraps(func)
            def _run(*args, **kwargs):
                _, hard = resource.getrlimit(resource.RLIMIT_NPROC)
                resource.setrlimit(resource.RLIMIT_NPROC,
                                   (min(nproc, 4096), hard))
                return func(*args, **kwargs)
            return _run
        return wrap


def now(monotonic=False):
    if hasattr(time, 'monotonic') and monotonic:
        # from system start, interrupt by reboot
        return time.monotonic()
    return time.time()


def set_nproc(nproc=4096):
    _, hard = resource.getrlimit(resource.RLIMIT_NPROC)
    resource.setrlimit(resource.RLIMIT_NPROC, (min(nproc, 4096), hard))


def set_memory(size=-1):
    _, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (int(size), hard))


def set_runtime(seconds=sys.maxsize):
    signal.signal(signal.SIGALRM, runtime_exceeded)
    signal.alarm(seconds)
