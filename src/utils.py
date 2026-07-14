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
import inspect
import tempfile
import argparse
import textwrap
import threading
import traceback
import subprocess
import contextlib
import importlib.util

from string import Template
from ast import literal_eval
from itertools import cycle
from datetime import datetime
from fractions import Fraction
from queue import Queue, Empty
from threading import Thread, Lock, RLock
from argparse import _SubParsersAction
from importlib.metadata import distribution
from collections.abc import MutableSet, Iterable
from functools import total_ordering, wraps, partial
from subprocess import check_output, call, Popen, PIPE
from collections import Counter, deque, OrderedDict, defaultdict

from os.path import (
    dirname,
    basename,
    isfile,
    isdir,
    exists,
    normpath,
    realpath,
    abspath,
    split,
    splitext,
    join,
    expanduser,
)

from .logger import *
from .limiter import *

BACKEND = ["local", "localhost", "sge", "slurm"]

NOT_ALPHA_DIGIT = re.compile("[^0-9A-Za-z]")
SBATCH_JOB_ID_DECODER = re.compile(r"Submitted batch job (\d+)")
QSUB_JOB_ID_DECODER = re.compile(r"Your job (\d+) \(.+?\) has been submitted")
TIMEDELTA_REGEX = re.compile(r'^((?P<weeks>[\.\d]+?)w)? *'
                             r'((?P<days>[\.\d]+?)d)? *'
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

DEFAULT_MAX_SUBMIT_PER_SEC = 20
DEFAULT_MAX_CHECK_PER_SEC = 5


class JobFailedError(Exception):
    """Raised when one or more jobs have failed. Carries the list of failed jobs."""

    def __init__(self, msg="", jobs=None):
        self.msg = msg
        self.failed_jobs = jobs and [j for j in jobs if j.is_fail]

    def __str__(self):
        if self.msg:
            return self.msg
        fj = self.failed_jobs
        fj_names = [j.jobname for j in fj]
        fj_logs = [j.logfile for j in fj]
        s = f"{len(fj)} jobs {fj_names} failed, please check in logs: {fj_logs}"
        return style(s, fore="red", mode="bold")


class RunJobException(Exception):
    """Base exception for all runjob errors, rendered with red bold styling."""

    def __init__(self, msg=""):
        self.msg = msg

    def __str__(self):
        return style(self.msg, fore="red", mode="bold")

    __repr__ = __str__


class RunJobError(RunJobException):
    """Fatal runjob error — terminates the run."""
    pass


class JobError(RunJobException):
    """A single job encountered an error."""
    pass


class JobOrderError(RunJobException):
    """Dependency/ordering specification is invalid."""
    pass


class MaxRetryError(RunJobException):
    """Maximum retry count exceeded for one or more jobs."""
    pass


class JobQueue(Queue):
    """A FIFO queue backed by an :class:`OrderedSet`, with deduplication and named retrieval."""

    def _init(self, maxsize):
        self._queue = OrderedSet()

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
        items = ', '.join(map(repr, self._queue))
        return f'{self.__class__.__name__}([{items}])'

    __repr__ = __str__

    @property
    def length(self):
        """Number of items currently in the queue."""
        return self.qsize()

    @property
    def queue(self):
        """Return a shallow copy of the underlying ordered set."""
        return self._queue.copy()

    def puts(self, *items, **kw):
        """Enqueue multiple items at once."""
        for item in items:
            self.put(item, **kw)

    def get(self, name=None, block=True, timeout=None):
        """Dequeue an item, optionally by name."""
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
                endtime = now(1) + timeout
                while not self._qsize():
                    remaining = endtime - now(1)
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get(name)
            self.not_full.notify()
            return item


class ParseSingal(Thread):
    """Background thread that installs signal handlers for graceful shutdown."""

    def __init__(self, obj=None):
        super(ParseSingal, self).__init__()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGUSR1, self.signal_handler_us)
        # ignore SIGCHLD
        # signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        self.daemon = True
        self.obj = obj

    def run(self):
        """Minimal run to satisfy the Thread interface (signals are handled in the main thread)."""
        time.sleep(1)

    def obj_exit(self):
        """Trigger the associated object's safe-exit procedure."""
        if self.obj is not None:
            self.obj.signaled = True
            self.obj.safe_exit()

    def signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM — exit gracefully."""
        self.obj_exit()
        # os._exit(signum)
        sys.exit(signum)

    def signal_handler_us(self, signum, frame):
        """Handle SIGUSR1 — raise :class:`RunJobError` to propagate to the main thread."""
        self.obj_exit()
        if self.obj is not None:
            raise RunJobError(self.obj.err_msg)
        else:
            sys.exit(signum)


class RunThread(Thread):
    """Thread subclass that captures the return value of the target function."""

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
        """Execute the target function, capturing any exception."""
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
        """Signal the thread to stop."""
        if not self.stopped():
            self._stop_event.set()

    def stopped(self):
        """Return True if the stop event has been set."""
        return self._stop_event.is_set()


class DummyFile(object):
    """File-like object that silently discards all writes (used to suppress stdout)."""
    def write(self, x):
        """Discard the written data (no-op)."""
        pass


def style(string, mode='', fore='', back=''):
    """Wrap *string* with ANSI escape codes for color and text mode.

    Args:
        string: The text to style.
        mode: Text mode — ``"bold"``, ``"dark"``, ``"underline"``, ``"blink"``, ``"reverse"``, ``"concealed"``.
        fore: Foreground color — ``"red"``, ``"green"``, ``"yellow"``, ``"blue"``, ``"purple"``, ``"cyan"``, ``"white"``, ``"gray"``.
        back: Background color (same options as *fore*).
    """
    mode = f'{STYLE["mode"].get(mode, "")}'
    fore = f'{STYLE["fore"].get(fore, "")}'
    back = f'{STYLE["back"].get(back, "")}'
    style = ';'.join([s for s in [mode, fore, back] if s])
    style = f'\033[{style}m' if style else ''
    end = f'\033[{STYLE["default"]["end"]}m' if style else ''
    return f'{style}{string}{end}'


REQUIRED = style("(required)", fore="green", mode="bold")


class mute(object):
    """Decorator/descriptor that suppresses stdout during the wrapped call."""

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


def retry(func=None, *, max_num=3, delay=5, callback=None):
    """Decorator: retry a function up to *max_num* times with *delay* seconds between attempts.

    Can be used as ``@retry`` or ``@retry(max_num=5, delay=2)``.
    """
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
                    log.warning(f"retry {try_num-1}")
                res = func(*args, **kwargs)
            except Exception as e:
                if try_num > 1:
                    log.error(f"retry {try_num-1} error, {e}")
                else:
                    log.error(e)
                if try_num <= max_num:
                    time.sleep(delay)
                continue
            else:
                break
        else:
            raise MaxRetryError(f"max retry {max_num} error")
        if callback:
            return callback(res)
        return res
    return wrapper


def inner_indent(text, tab_sz, tab_chr=' '):
    """Indent all lines of *text* except the first by *tab_sz* spaces."""
    def indented_lines():
        for i, line in enumerate(text.splitlines(True)):
            yield (
                tab_chr * tab_sz + line if line.strip() else line
            ) if i else line
    return ''.join(indented_lines())


def argvhelp(func=None, *, arglen=None):
    """Decorator: show help (from the function's docstring) when no/invalid args are given."""
    if func is None:
        return partial(argvhelp, arglen=arglen)

    @wraps(func)
    def wrapper(*args, **kwargs):

        if len(sys.argv) == 1 or "-h" in sys.argv or "--help" in sys.argv or arglen and len(sys.argv) != arglen+1:
            msg = textwrap.dedent(f"""
                {style("Usage:", fore="red", mode="bold")}

                    {inner_indent(style(func.__doc__.strip(), mode="bold"), 16)}

                """)
            sys.exit(msg)
        return func(*args, **kwargs)

    return wrapper


dochelp = helpdoc = argvhelp


def get_job_state(state):
    """Return an ANSI-colored version of a job status string."""
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


def human_size(num, deg=1024):
    """Convert a byte count to a human-readable string (e.g. ``"4.0G"``).

    Args:
        num: Number of bytes.
        deg: Base — 1024 for binary units, 1000 for decimal.
    """
    deg = float(deg)
    for unit in ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < deg:
            return f"{num:3.1f}{unit}"
        num /= deg
    return f'{num:.1f}Y'


def human_size_parse(size):
    """Parse a human-readable size string (e.g. ``"4G"``, ``"8192M"``) into bytes."""
    s, u = re.search(r"(\d+(?:\.\d+)?)(\D*)", str(size)).group(1, 2)
    s = float(s)
    if s < 1 and not u:
        u = "M"
    if u:
        for unit in ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if u.upper().strip()[0] == unit:
                return int(s)
            s *= u.islower() and 1000 or 1024
    else:
        return int(s)


def seconds2human(s):
    """Convert seconds to ``H:MM:SS.ss`` string."""
    m, s = divmod(s, 60)
    h, m = divmod(int(m), 60)
    return f"{h:d}:{m:02d}:{s:04.2f}"


def human2seconds(time_str):
    """Parse human-readable time string to seconds.

    Valid strings: '8h', '2d 8h 5m 2s', '2m4.3s'.
    """
    if time_str is None:
        return sys.maxsize
    parts = TIMEDELTA_REGEX.match(str(time_str))
    if parts is None:
        return sys.maxsize
    try:
        time_params = {name: float(param)
                       for name, param in parts.groupdict().items() if param}
        return int(sum(MULTIPLIERS[k]*v for k, v in time_params.items()))
    except (ValueError, TypeError):
        return sys.maxsize


def nestdict():
    """Return a recursively-nesting :class:`defaultdict` (infinite-depth auto-vivification)."""
    return defaultdict(nestdict)


def mkdir(*path):
    """Create directories, silently ignoring errors (e.g. already exists)."""
    for p in path:
        if not isdir(p):
            try:
                os.makedirs(p)
            except:
                pass


def touch(fname, times=None):
    """Create an empty file or update its mtime (like the Unix ``touch`` command)."""
    with open(fname, 'a'):
        os.utime(fname, times)


def is_entry_cmd():
    """Return True if the current process was invoked via a registered entry point."""
    prog = abspath(realpath(sys.argv[0]))
    return basename(prog) in \
        [e.name for e in distribution(__package__).entry_points] \
        and join(sys.prefix, "bin", basename(prog)) == prog


def terminate_process(pid, sig=signal.SIGKILL, recursive=True):
    """Kill a process tree by PID.

    Args:
        pid: Process ID.
        sig: Signal to send (default ``SIGKILL``).
        recursive: If True, also signal all child processes.
    """
    try:
        pproc = psutil.Process(pid)
        if recursive:
            for cproc in pproc.children(recursive=True):
                # cproc.terminate() # SIGTERM
                # cproc.kill()  # SIGKILL
                cproc.send_signal(sig)
        # pproc.terminate()
        # pproc.kill()
        pproc.send_signal(sig)
    except:
        pass


def is_running(pid=None):
    """Return ``True`` if the process with the given PID is still running."""
    try:
        p = psutil.Process(pid)
    except:
        return False
    else:
        return p.is_running()


def call_cmd(cmd, verbose=False, run=True, daemon=False, timeout=None, max_memory=-1, exception_ok=False):
    """Execute a shell command via subprocess.

    Args:
        cmd: Shell command string or argument list.
        verbose: If True, capture and display output.
        run: If False, only print the command without executing it.
        daemon: If True, use ``Popen`` (non-blocking); otherwise ``call``.
        timeout: Subprocess timeout in seconds.
        max_memory: Memory limit for the child process.
        exception_ok: If True, return ``None`` on error instead of raising.
    """
    if not run:
        print(cmd)
        return
    func_name = "Popen" if daemon else "call"
    proc = None
    try:
        proc = getattr(subprocess, func_name)(cmd, shell=isinstance(cmd, str), timeout=timeout,
                                              stdout=not verbose and -3 or None, stderr=-2, preexec_fn=partial(set_memory, max_memory))
    except Exception as err:
        if exception_ok:
            return proc
        else:
            raise err
    else:
        return proc


def which(program):
    """Locate an executable on ``PATH`` (similar to the Unix ``which`` command)."""
    ex = dirname(sys.executable)
    found_path = None
    fpath, fname = split(program)
    if fpath:
        program = canonicalize(program)
        if is_exe(program):
            found_path = program
    else:
        if is_exe(join(ex, program)):
            return join(ex, program)
        from shutil import which as _which
        found_path = _which(program)
    return found_path


def which_exe(program, paths=None):
    """Locate an executable in specific directories (or ``PATH``), returning the full path."""
    ex = dirname(sys.executable)
    found_path = None
    fpath, fname = split(program)
    if fpath:
        program = canonicalize(program)
        if is_exe(program):
            found_path = program
    else:
        if is_exe(join(ex, program)):
            return join(ex, program)
        paths_to_search = []
        if isinstance(paths, (tuple, list)):
            paths_to_search.extend(paths)
        else:
            env_paths = os.environ.get("PATH", "").split(os.pathsep)
            paths_to_search.extend(env_paths)
        for path in paths_to_search:
            exe_file = join(canonicalize(path), program)
            if is_exe(exe_file):
                found_path = exe_file
                break
    return found_path


def is_exe(file_path):
    """Return True if *file_path* exists and is executable."""
    return (
        exists(file_path)
        and os.access(file_path, os.X_OK)
        and isfile(realpath(file_path))
    )


def canonicalize(path):
    """Expand ``~``, environment variables, and return a real absolute path."""
    return abspath(expanduser(path))


_is_sge_submit_cache = None


def is_sge_submit():
    """Return True if the current host is an SGE submit node (has ``qsub`` access)."""
    global _is_sge_submit_cache
    if _is_sge_submit_cache is not None:
        return _is_sge_submit_cache
    if os.getenv("SGE_ROOT") and which("qconf"):
        hostname = splitext(socket.gethostname())[0]
        try:
            with os.popen("qconf -ss") as fi:
                for line in fi:
                    if line.strip() == hostname or splitext(line.strip())[0] == hostname:
                        _is_sge_submit_cache = True
                        return True
        except:
            _is_sge_submit_cache = False
            return False
    _is_sge_submit_cache = False
    return False


_is_slurm_host_cache = None


def is_slurm_host():
    """Return True if the current host has Slurm commands (``sinfo``, ``squeue``) available."""
    global _is_slurm_host_cache
    if _is_slurm_host_cache is not None:
        return _is_slurm_host_cache
    _is_slurm_host_cache = bool(which("sinfo") and which("sbatch") and which("scancel"))
    return _is_slurm_host_cache


_default_slurm_queue_cache = None


def default_slurm_queue():
    """Return the default Slurm partition/queue for the current user."""
    global _default_slurm_queue_cache
    if _default_slurm_queue_cache is not None:
        return _default_slurm_queue_cache
    q = None
    if which("sinfo"):
        try:
            with os.popen("sinfo -h | awk '{print $1}'") as fi:
                q = sorted(set([i.strip("*") for i in fi.read().split()]))
        except:
            pass
    _default_slurm_queue_cache = q
    return q


_default_slurm_node_cache = None


def default_slurm_node():
    """Return the default Slurm node name from ``/etc/slurm/slurm.conf``."""
    global _default_slurm_node_cache
    if _default_slurm_node_cache is not None:
        return _default_slurm_node_cache
    node = None
    if which("sinfo"):
        try:
            with os.popen("sinfo -Nh | awk '{print $1}'") as fi:
                node = sorted(set([i.strip("*") for i in fi.read().split()]))
        except:
            pass
    _default_slurm_node_cache = node
    return node


def default_backend():
    """Detect and return the default job submission backend (sge/slurm/localhost)."""
    if is_sge_submit():  # sge first
        return "sge"
    elif is_slurm_host():
        return "slurm"
    return "localhost"


class AppDirs(object):
    """Convenience wrapper for getting application dirs."""

    def __init__(self, appname=None, version=None):
        self.appname = appname
        self.version = version

    @property
    def user_data_dir(self):
        """Platform-specific user data directory."""
        path = os.getenv('XDG_DATA_HOME', os.path.expanduser("~/.local/share"))
        return self.__user_dir(path)

    @property
    def site_data_dir(self):
        """Platform-specific site data directory."""
        path = os.getenv('XDG_DATA_DIRS',
                         os.pathsep.join(['/usr/local/share', '/usr/share']))
        return self.__site_dir(path)

    @property
    def site_config_dir(self):
        """Platform-specific site config directory."""
        path = os.getenv('XDG_CONFIG_DIRS', '/etc/xdg')
        return self.__site_dir(path)

    @property
    def user_config_dir(self):
        """Platform-specific user config directory."""
        path = os.getenv('XDG_CONFIG_HOME', os.path.expanduser("~/.config"))
        return self.__user_dir(path)

    @property
    def user_cache_dir(self):
        """Platform-specific user cache directory."""
        path = os.getenv('XDG_CACHE_HOME', os.path.expanduser('~/.cache'))
        return self.__user_dir(path)

    @property
    def user_state_dir(self):
        """Platform-specific user state directory."""
        path = os.getenv('XDG_STATE_HOME',
                         os.path.expanduser("~/.local/state"))
        return self.__user_dir(path)

    @property
    def user_log_dir(self):
        """Platform-specific user log directory."""
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
    """Return the platform-appropriate user config directory for *app*."""
    app = AppDirs(app, version)
    return app.user_config_dir


class CmdTemplate(Template):
    """String template with ``$var`` substitution (like shell variables)."""
    delimiter = "$"
    idpattern = r"(?a:[_a-z][_\.a-z0-9]*)"


@total_ordering
class OrderedSet(OrderedDict, MutableSet):
    """A set that preserves insertion order, backed by :class:`OrderedDict`."""

    def __init__(self, items=""):
        for item in items:
            self.add(item)

    def update(self, *args):
        """Add all elements from each iterable argument."""
        for s in args:
            for e in s:
                self.add(e)

    def add(self, elem):
        """Add an element to the set."""
        self[elem] = None

    append = add

    def extend(self, elems):
        """Add all elements from the iterable *elems*."""
        for e in elems:
            self.add(e)

    def discard(self, elem):
        """Remove *elem* if present, do nothing otherwise."""
        self.pop(elem, None)

    def __lt__(self, other):
        return all(e in other for e in self) and self != other

    def __eq__(self, other):
        if len(self) != len(other):
            return False
        return all(i == j for i, j in zip(self, other))

    def __repr__(self):
        items = ', '.join(map(repr, self.keys()))
        return f'{self.__class__.__name__}([{items}])'

    __str__ = __repr__


def exception_hook(et, ev, eb):
    """Minimal excepthook: print the exception type and message in red bold."""
    err = f'{et.__name__}: {ev}'
    print(style(err, fore="red", mode="bold"))


def suppress_exceptions(*expts, msg="", trace_exception=True):
    """Decorator: catch specified exceptions and re-raise as :class:`RuntimeError`."""
    def outer_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sys.excepthook = trace_exception and sys.__excepthook__ or exception_hook
            try:
                res = func(*args, **kwargs)
            except expts as e:
                err = msg or str(e)
                exc = RuntimeError(err)
                exc.__cause__ = None
                raise exc
            else:
                return res
            finally:
                sys.excepthook = sys.__excepthook__
        return wrapper
    return outer_wrapper


@contextlib.contextmanager
def suppress_stdout_stderr():
    """Context manager that redirects stdout and stderr to ``/dev/null``."""
    with open(os.devnull, 'w') as fnull:
        with contextlib.redirect_stderr(fnull) as err, contextlib.redirect_stdout(fnull) as out:
            yield (err, out)


@contextlib.contextmanager
def tmp_chdir(dest):
    """Context manager: temporarily change working directory to *dest*, then restore."""
    curdir = os.getcwd()
    try:
        os.chdir(dest)
        yield
    finally:
        os.chdir(curdir)


@contextlib.contextmanager
def add_to_sys_path(path):
    """Context manager: temporarily prepend *path* to ``sys.path``."""
    if path in sys.path:
        already_add = True
    else:
        sys.path.insert(0, path)
        already_add = False
    try:
        yield
    finally:
        if not already_add:
            sys.path.remove(path)


def check_module_exists(name):
    """Return True if the named module can be imported."""
    return importlib.util.find_spec(name) is not None


class SuppressStdoutStderr(object):
    """Context manager that suppresses stdout/stderr at the file-descriptor level.

    More thorough than :func:`suppress_stdout_stderr` — works for subprocess output too.
    """

    def __enter__(self):
        self.outnull_file = open(os.devnull, 'w')
        self.errnull_file = open(os.devnull, 'w')

        self.old_stdout_fileno_undup = sys.stdout.fileno()
        self.old_stderr_fileno_undup = sys.stderr.fileno()

        self.old_stdout_fileno = os.dup(sys.stdout.fileno())
        self.old_stderr_fileno = os.dup(sys.stderr.fileno())

        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr

        os.dup2(self.outnull_file.fileno(), self.old_stdout_fileno_undup)
        os.dup2(self.errnull_file.fileno(), self.old_stderr_fileno_undup)

        sys.stdout = self.outnull_file
        sys.stderr = self.errnull_file
        return self

    def __exit__(self, *_):
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

        os.dup2(self.old_stdout_fileno, self.old_stdout_fileno_undup)
        os.dup2(self.old_stderr_fileno, self.old_stderr_fileno_undup)

        os.close(self.old_stdout_fileno)
        os.close(self.old_stderr_fileno)

        self.outnull_file.close()
        self.errnull_file.close()


def safe_cycle(itr):
    """Thread-safe version of :func:`itertools.cycle` using a lock."""
    it = cycle(itr)
    lock = Lock()
    while True:
        with lock:
            yield next(it)


def string_num_orders(s):
    """Sort key: split strings into numeric and text parts for natural ordering."""
    out = []
    for p in NOT_ALPHA_DIGIT.split(s):
        try:
            out.append(float(p))
        except ValueError:
            out.append(p)
    return out


def free_disk_space(path=None):
    """Return human-readable free disk space at *path* (or current directory)."""
    ph = path or os.getcwd()
    return human_size(os.statvfs(ph).f_bfree * os.statvfs(ph).f_frsize)


def converter(in_str):
    """Safely convert a string to a Python literal via :func:`ast.literal_eval`, falling back to the original string."""
    try:
        out = literal_eval(in_str)
    except Exception:
        out = in_str
    return out


def common_substring(strings):
    """Find the longest common substring among all strings.

    Uses a greedy search from the shortest string: tries substrings from
    longest to shortest, returning the first that appears in all strings.
    """
    if not strings:
        return ""
    if len(strings) == 1:
        return strings[0]
    shortest = min(strings, key=len)
    for length in range(len(shortest), 0, -1):
        seen = set()
        for i in range(len(shortest) - length + 1):
            sub = shortest[i:i + length]
            if sub not in seen:
                seen.add(sub)
                if all(sub in s for s in strings):
                    return sub
    return ""


def get_common_suffpref(l, order=1):
    """Return the longest common prefix (order=1) or suffix (order=-1) among a list of strings."""
    common_string = 0
    max_length = min([len(i) for i in l])
    for i in range(max_length):
        index = i
        if order == -1:
            index = i+1
        if all(map(lambda x: x[order*index] == l[0][order*index], l)):
            common_string += 1
        else:
            break
    if order == 1 or common_string == 0:
        return l[0][:common_string]
    else:
        return l[0][-common_string:]


def eval_value(obj):
    """Convert a string to its most likely Python type (int, float, bool, None)."""
    if isinstance(obj, dict):
        return {k: eval_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [eval_value(elem) for elem in obj]
    if isinstance(obj, str):
        if obj.upper() == 'NONE':
            return None
        if obj.isnumeric():
            return int(obj)
        if obj.replace('.', '', 1).isnumeric():
            return float(obj)
        if obj.upper() in ('TRUE', 'FALSE', 'T', 'F'):
            return obj.upper() in ('TRUE', 'T')
    return converter(obj)


def option_on_command_line(args=None, prefix_chars="-", option_strings=None):
    """Check whether an option was explicitly given on the command line."""
    args = args or sys.argv[1:]
    if not option_strings or not isinstance(option_strings, (list, tuple)):
        return False
    arg_names = []
    for arg_string in args:
        if arg_string and arg_string[0] in prefix_chars and "=" in arg_string:
            option_string, explicit_arg = arg_string.split("=", 1)
            arg_names.append(option_string)
        else:
            arg_names.append(arg_string)
    return any(potential_arg in arg_names for potential_arg in option_strings)


class TempFile(object):
    """A temporary file that can be explicitly deleted, with context-manager support."""

    def __init__(self, suffix=None, prefix=None, dir=None):
        if dir and (not isdir(dir) or not os.access(dir, os.W_OK | os.X_OK)):
            dir = None
        self.temp = tempfile.NamedTemporaryFile(
            suffix=suffix, prefix=prefix, delete=False, dir=dir)

    def delete(self):
        """Close and delete the temporary file from disk."""
        try:
            self.temp._closer.delete = True
        except:
            self.temp.delete = True
        self.temp.close()

    def _get_name(self):
        return self.temp.name

    @property
    def name(self):
        """The absolute path of the temporary file."""
        return self._get_name()

    def __exit__(self, type, value, traceback):
        try:
            self.delete()
        except AttributeError:
            pass
        finally:
            self.delete()

    def __enter__(self):
        return self


CONF_FILE_NAME = "config.ini"
USER_CONF_FILE = join(user_config_dir(), CONF_FILE_NAME)
PKG_CONF_FILE = join(dirname(abspath(__file__)), CONF_FILE_NAME)


def dont_write_bytecode(func):
    """Decorator: temporarily set ``sys.dont_write_bytecode = True`` while calling *func*."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        ori = sys.dont_write_bytecode
        try:
            sys.dont_write_bytecode = True
            return func(*args, **kwargs)
        finally:
            sys.dont_write_bytecode = ori
    return wrapper


@dont_write_bytecode
def load_module_from_path(path, add_to_sys=True):
    """Dynamically import a Python module from a file path without generating ``.pyc``."""
    path = os.path.abspath(path)
    _, filename = os.path.split(path)
    module_name, _ = os.path.splitext(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    if add_to_sys:
        sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class SingletonType(type):
    """Metaclass that enforces a single instance per class (thread-safe)."""

    _instance_lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        super(SingletonType, self).__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if not hasattr(self, "_instance"):
            with self._instance_lock:
                self._instance = super(
                    SingletonType, self).__call__(*args, **kwargs)
        return self._instance


def isiterable(obj):
    """Return True if *obj* is iterable (not a string)."""
    return not isinstance(obj, str) and isinstance(obj, Iterable)


def flatten(x):
    """Recursively flatten a nested iterable into a flat list."""
    return [y for l in x for y in flatten(
        l)] if isiterable(x) else [x]


def flatten_json(nested_json, exclude=[''], sep='.'):
    """Flatten a nested dict into ``{"a.b.c": value}`` style keys."""
    out = dict()

    def _flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude:
                    _flatten(x[a], f'{name}{a}{sep}')
        elif type(x) is list:
            i = 0
            for a in x:
                _flatten(a, f'{name}{i}{sep}')
                i += 1
        else:
            out[name[:-1]] = x
    _flatten(nested_json)
    return out


def set_nested_value_loop(d, keys_str, value):
    """Set ``d["a"]["b"]["c"] = value`` from a dotted key string ``"a.b.c"``, creating intermediate dicts as needed."""
    keys = keys_str.split('.')
    current = d
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def nested_json(flatten_json):
    """Convert flattened JSON dict to nested dict structure.

    Example:
        nested_json({'a.b.c': 1, 'c.x': 1, 'c.y': {'m.n': 2}})
        == {'a': {'b': {'c': 1}}, 'c': {'x': 1, 'y': {'m': {'n': 2}}}}
    """
    out = {}
    for k, v in flatten_json.items():
        if isinstance(v, dict):
            v = nested_json(v)
        set_nested_value_loop(out, k, v)
    return out


def chunk(lst, size=80):
    """Split *lst* into sublists of at most *size* elements."""
    return [lst[i:i+size] for i in range(0, len(lst), size)]


def remove_argument(parser, flag=None, sub=None):
    """Remove an argument (and optionally a subcommand) from an argparse parser."""
    for action in parser._action_groups:
        for group_action in action._group_actions[:]:
            opts = group_action.option_strings
            if isinstance(group_action, _SubParsersAction):
                if sub and sub in group_action.choices:
                    if flag:
                        sub_parser = group_action.choices[sub]
                        remove_argument(sub_parser, flag, sub)
                    else:
                        for choices_action in group_action._choices_actions[:]:
                            if choices_action.dest == sub:
                                group_action._choices_actions.remove(
                                    choices_action)
                else:
                    for cmd, sub_parser in group_action.choices.items():
                        remove_argument(sub_parser, flag, cmd)
            if (opts and flag in opts) or group_action.dest == flag:
                parser._actions.remove(group_action)
                action._group_actions.remove(group_action)


def to_async(func):
    """Wrap a synchronous function into an async coroutine."""
    if is_async_callable(func):
        return func
    import asyncio

    @wraps(func)
    async def wrapper(*args, loop=None, executor=None, **kwargs):
        # default executor: concurrent.futures.ThreadPoolExecutor
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return wrapper


def to_sync(coroutine):
    """Wrap an async coroutine into a synchronous function."""
    if not is_async_callable(coroutine):
        return coroutine
    import asyncio

    @wraps(coroutine)
    def wrapper(*args, **kwargs):
        return asyncio.run(coroutine(*args, **kwargs))
    return wrapper


def async_map(coroutine_func, *iterables):
    """Run an async coroutine function in parallel over iterables."""
    import asyncio
    coroutine_func = to_async(coroutine_func)

    async def _run(tasks):
        return await asyncio.gather(*tasks)
    fs = [coroutine_func(args) for args in iterables]
    return asyncio.run(_run(fs))


def process_map(func, *iterables, timeout=None, chunksize=1, **kw):
    """Parallel ``map`` using :class:`~concurrent.futures.ProcessPoolExecutor`."""
    from concurrent.futures import ProcessPoolExecutor
    with ProcessPoolExecutor(**kw) as executor:
        result = list(executor.map(func, iterables,
                      timeout=timeout, chunksize=chunksize))
    return result


def thread_map(func, *iterables, timeout=None, chunksize=1, **kw):
    """Parallel ``map`` using :class:`~concurrent.futures.ThreadPoolExecutor` (sync or async)."""
    from concurrent.futures import ThreadPoolExecutor
    func = to_sync(func)
    with ThreadPoolExecutor(**kw) as executor:
        result = list(executor.map(func, iterables,
                      timeout=timeout, chunksize=chunksize))
    return result


def is_async_callable(obj):
    """Return True if *obj* is an async function or a callable with an async ``__call__``."""
    while isinstance(obj, partial):
        obj = obj.func
    return inspect.iscoroutinefunction(obj) or (
        callable(obj) and inspect.iscoroutinefunction(
            getattr(obj, "__call__", None))
    )


def do_test(test_case, test_func_name=None, verbosity=1):
    """Convenience function to run unittest test cases.

    Args:
        test_case: A subclass of :class:`unittest.TestCase` (the class itself, not an instance).
        test_func_name: Optional name of a specific test method (must start with "test_").
                        If None, all test methods in the TestCase are run.
        verbosity: Output verbosity level — 0=quiet, 1=default, 2=detailed. Default 1.

    Returns:
        bool: ``True`` if all tests pass, ``False`` if any fail.
        None: If *test_case* is not a TestCase subclass, or *test_func_name* is invalid.
    """
    import unittest
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    if not issubclass(test_case, unittest.TestCase):
        return
    if not test_func_name:
        suite.addTest(loader.loadTestsFromTestCase(test_case))
    elif test_func_name.startswith("test_") and hasattr(test_case, test_func_name):
        suite.addTest(test_case(test_func_name))
    else:
        return
    runner = unittest.TextTestRunner(verbosity=verbosity)
    _test = runner.run(suite)
    return _test.wasSuccessful()
