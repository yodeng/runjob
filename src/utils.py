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

from datetime import datetime
from fractions import Fraction
from threading import Thread, Lock
from collections import Counter, deque
from functools import total_ordering, wraps, partial
from subprocess import check_output, call, Popen, PIPE
from os.path import dirname, basename, isfile, isdir, exists, normpath, realpath, abspath, splitext, join, expanduser

from .loger import *
from .config import which
from ._version import __version__

PY3 = sys.version_info.major == 3

if not PY3:
    from Queue import Queue, Empty
else:
    from queue import Queue, Empty


QSUB_JOB_ID_DECODER = re.compile("Your job (\d+) \(.+?\) has been submitted")


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
        return "{} jobs {} failed, please check in logs: {}".format(len(fj), fj_names, fj_logs)


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


class MaxRetryError(Exception):
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


REQUIRED = style("(required)", fore="green", mode="bold")


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


def mkdir(*path):
    for p in path:
        if not isdir(p):
            try:
                os.makedirs(p)
            except:
                pass


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


def show_help_on_empty_command():
    if len(sys.argv) == 1:
        sys.argv.append('--help')


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


def default_args():
    p = argparse.ArgumentParser(add_help=False)
    base = p.add_argument_group("base arguments")
    base.add_argument('-v', '--version',
                      action='version', version="v" + __version__)
    base.add_argument("-j", "--jobfile", type=argparse.FileType('r'), nargs="?", default=sys.stdin,
                      help="input jobfile, if empty, stdin is used. " + REQUIRED, metavar="<jobfile>")
    base.add_argument("-n", "--num", type=int,
                      help="the max job number runing at the same time. (default: all of the jobfile, max 1000)", metavar="<int>")
    base.add_argument("-s", "--startline", type=int,
                      help="which line number(1-base) be used for the first job. (default: %(default)s)", metavar="<int>", default=1)
    base.add_argument("-e", "--endline", type=int,
                      help="which line number (include) be used for the last job. (default: last line of the jobfile)", metavar="<int>")
    base.add_argument('-d', '--debug', action='store_true',
                      help='log debug info.', default=False)
    base.add_argument("-l", "--log", type=str,
                      help='append log info to file. (default: stdout)', metavar="<file>")
    base.add_argument('-r', '--retry', help="retry N times of the error job, 0 or minus means do not re-submit.",
                      type=int, default=0, metavar="<int>")
    base.add_argument('-ivs', '--retry-ivs', help="retry the error job after N seconds.",
                      type=int, default=2, metavar="<int>")
    base.add_argument("-f", "--force", default=False, action="store_true",
                      help="force to submit jobs even if already successed.")
    base.add_argument("--dot", action="store_true", default=False,
                      help="do not execute anything and print the directed acyclic graph of jobs in the dot language.")
    base.add_argument("--local", default=False, action="store_true",
                      help="submit your jobs in localhost, same as '--mode local'.")
    base.add_argument("--strict", action="store_true", default=False,
                      help="use strict to run, means if any errors, clean all jobs and exit.")
    base.add_argument("--quiet", action="store_true", default=False,
                      help="suppress all output and logging")
    base.add_argument('--max-check', help="maximal number of job status checks per second, fractions allowed.",
                      type=float, default=3, metavar="<float>")
    base.add_argument('--max-submit', help="maximal number of jobs submited per second, fractions allowed.",
                      type=float, default=30, metavar="<float>")
    timeout_action_args(p)
    return p


def timeout_action_args(parser):
    time_args = parser.add_argument_group("time control arguments")
    time_args.add_argument('--max-queue-sec', help="maximal seconds between submit and running per job, (default: no-limiting)",
                           type=int, default=sys.maxsize, metavar="<int>")
    time_args.add_argument('--max-run-sec', help="maximal seconds start from running per job, (default: no-limiting)",
                           type=int, default=sys.maxsize, metavar="<int>")
    time_args.add_argument('--max-wait-sec', help="maximal seconds start from submit per job, (default: no-limiting)",
                           type=int, default=sys.maxsize, metavar="<int>")
    time_args.add_argument('--max-timeout-retry', help="retry N times for the timeout error job, 0 or minus means do not re-submit.",
                           type=int, default=0, metavar="<int>")


def sge_args(parser):
    sge = parser.add_argument_group("sge arguments")
    sge.add_argument("-q", "--queue", type=str, help="the queue your job running, multi queue can be sepreated by whitespace. (default: all accessed queue)",
                     nargs="*", metavar="<queue>")
    sge.add_argument("-m", "--memory", type=int,
                     help="the memory used per command (GB).", default=1, metavar="<int>")
    sge.add_argument("-c", "--cpu", type=int,
                     help="the cpu numbers you job used.", default=1, metavar="<int>")


def batchcmp_args(parser):
    batchcmp = parser.add_argument_group("batchcompute arguments")
    batchcmp.add_argument("-om", "--out-maping", type=str,
                          help='the oss output directory if your mode is "batchcompute", all output file will be mapping to you OSS://BUCKET-NAME. if not set, any output will be reserved.', metavar="<dir>")
    batchcmp.add_argument('--access-key-id', type=str,
                          help="AccessKeyID while access oss.", metavar="<str>")
    batchcmp.add_argument('--access-key-secret', type=str,
                          help="AccessKeySecret while access oss.", metavar="<str>")
    batchcmp.add_argument('--region', type=str, default="beijing", choices=['beijing', 'hangzhou', 'huhehaote', 'shanghai',
                                                                            'zhangjiakou', 'chengdu', 'hongkong', 'qingdao', 'shenzhen'], help="batch compute region.")


def runsgeArgparser():
    parser = argparse.ArgumentParser(
        description="%(prog)s is a tool for managing parallel tasks from a specific shell scripts runing in localhost, sge or batchcompute.",
        parents=[default_args()],
        formatter_class=CustomHelpFormatter,
        allow_abbrev=False)
    parser.add_argument("-wd", "--workdir", type=str, help="work dir.",
                        default=abspath(os.getcwd()), metavar="<workdir>")
    parser.add_argument("-N", "--jobname", type=str,
                        help="job name. (default: basename of the jobfile)", metavar="<jobname>")
    parser.add_argument("-lg", "--logdir", type=str,
                        help='the output log dir. (default: "%s/%s_*_log_dir")' % (os.getcwd(), "%(prog)s"), metavar="<logdir>")
    parser.add_argument("-g", "--groups", type=int, default=1,
                        help="N lines to consume a new job group.", metavar="<int>")
    parser.add_argument('--init', help="command before all jobs, will be running in localhost.",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--call-back', help="command after all jobs finished, will be running in localhost.",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost", "batchcompute"], help="the mode to submit your jobs, if no sge installed, always localhost.")
    parser.add_argument('-ini', '--ini',
                        help="input configfile for configurations search.", metavar="<configfile>")
    parser.add_argument("-config", '--config',   action='store_true',
                        help="show configurations and exit.",  default=False)
    sge_args(parser)
    batchcmp_args(parser)
    parser.description = style(
        parser.description, fore="red", mode="underline")
    return parser


def runjobArgparser():
    parser = argparse.ArgumentParser(
        description="%(prog)s is a tool for managing parallel tasks from a specific job file running in localhost or sge cluster.",
        parents=[default_args()],
        formatter_class=CustomHelpFormatter,
        allow_abbrev=False)
    parser.add_argument('-i', '--injname', help="job names you need to run. (default: all job names of the jobfile)",
                        nargs="*", type=str, metavar="<str>")
    parser.add_argument("-m", '--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost"], help="the mode to submit your jobs, if no sge installed, always localhost.")
    parser.description = style(
        parser.description, fore="red", mode="underline")
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
    parser.add_argument('--max-runtime', type=int)
    parser.add_argument('--max-runtime-retry', type=int)
    return parser.parse_known_args(arglist)[0]


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


class CustomHelpFormatter(argparse.HelpFormatter):

    def _get_help_string(self, action):
        """Place default and required value in help string."""
        h = action.help

        # Remove any formatting used for Sphinx argparse hints.
        h = h.replace('``', '')

        if '%(default)' not in action.help and "default:" not in action.help:
            if action.default != '' and action.default != [] and \
                    action.default is not None and \
                    not isinstance(action.default, bool) and \
                    not isinstance(action.default, io.IOBase):
                if action.default is not argparse.SUPPRESS:
                    defaulting_nargs = [
                        argparse.OPTIONAL, argparse.ZERO_OR_MORE]

                    if action.option_strings or action.nargs in defaulting_nargs:
                        if '\n' in h:
                            lines = h.splitlines()
                            lines[0] += ' (default: %(default)s)'
                            h = '\n'.join(lines)
                        else:
                            h += ' (default: %(default)s)'
        if "required" not in action.help and hasattr(action, "required") and action.required:
            h += " " + REQUIRED
        return h

    def _format_action_invocation(self, action):
        """Removes duplicate ALLCAPS with positional arguments."""
        if not action.option_strings:
            default = self._get_default_metavar_for_positional(action)
            metavar, = self._metavar_formatter(action, default)(1)
            return metavar

        else:
            parts = []

            # if the Optional doesn't take a value, format is:
            #    -s, --long
            if action.nargs == 0:
                parts.extend(action.option_strings)

            # if the Optional takes a value, format is:
            #    -s ARGS, --long ARGS
            else:
                default = self._get_default_metavar_for_optional(action)
                args_string = self._format_args(action, default)
                for option_string in action.option_strings:
                    parts.append(option_string)

                return '%s %s' % (', '.join(parts), args_string)

            return ', '.join(parts)

    def _get_default_metavar_for_optional(self, action):
        return action.dest.upper()

    def _get_default_metavar_for_positional(self, action):
        return action.dest


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
