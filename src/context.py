import os
import sys
import types
import logging
import threading

from functools import wraps
from os.path import isfile, isdir, join, dirname, abspath

from .utils import mkdir
from .config import load_config
from .logger import getlog, Formatter


class ContextType(type):
    """Metaclass singleton — one ``Context`` instance per process."""

    _instance = None
    _instance_lock = threading.Lock()

    def __call__(self, *args, **kwargs):
        if self._instance is None:
            with self._instance_lock:
                if self._instance is None:          # double-checked locking
                    self._instance = super().__call__(*args, **kwargs)
        return self._instance


class Context(metaclass=ContextType):
    """Application-wide context (singleton)."""

    # class-level defaults — lazy-initialised so that module import is cheap
    _conf = None
    _log = None

    def __init__(self, *cf, init_bin=False, args=None, app=__package__, **kw):
        self.conf = load_config(
            *cf, init_bin=init_bin or kw.get("init_envs"), app=app)
        if cf or init_bin or kw.get("init_envs"):
            self.args = self.conf.args
        self.init_arg(args)
        self.init_log(name=app)

    # ── lazy class-level accessors ──────────────────────────────────

    @property
    def conf(self):
        if Context._conf is None:
            Context._conf = load_config()
        return Context._conf

    @conf.setter
    def conf(self, value):
        Context._conf = value

    @property
    def log(self):
        if Context._log is None:
            Context._log = getlog()
        return Context._log

    @log.setter
    def log(self, value):
        Context._log = value

    # ── convenience aliases (delegate through the instance ``conf``) ──

    @property
    def db(self):
        return self.conf.database

    @property
    def database(self):
        return self.conf.database

    @property
    def soft(self):
        return self.conf.software

    @property
    def exe(self):
        return self.conf.software

    @property
    def bin(self):
        return self.conf.software

    @property
    def software(self):
        return self.conf.software

    @property
    def args(self):
        return self.conf.args

    @args.setter
    def args(self, value):
        self.conf.args = value

    # ── internal helpers ────────────────────────────────────────────

    @classmethod
    def _ensure_conf(cls):
        if cls._conf is None:
            cls._conf = load_config()
        return cls._conf

    @classmethod
    def _ensure_log(cls):
        if cls._log is None:
            cls._log = getlog()
        return cls._log

    # ── class methods (operate on the singleton state) ─────────────

    @classmethod
    def add_config(cls, config=None):
        if config:
            cls._ensure_conf().update_config(config)

    @classmethod
    def add_bin(cls, bin_dir=None):
        if bin_dir:
            cls._ensure_conf()._bin_dirs.append(bin_dir)
        cls.init_bin()

    @classmethod
    def add_path(cls, path=None):
        p = join(sys.prefix, "bin") + ":" + os.environ["PATH"]
        if path and isdir(path):
            p = abspath(path) + ":" + p
        cls.add_envs(PATH=p)

    @classmethod
    def add_envs(cls, **kw):
        os.environ.update(kw)

    @classmethod
    def add_log(cls, logfile=None, create_if_not_exists=False):
        if logfile:
            logfile = abspath(logfile)
            if create_if_not_exists:
                mkdir(dirname(logfile))
            if isdir(dirname(logfile)):
                handler = logging.FileHandler(logfile, mode='a')
                handler.setFormatter(Formatter())
                cls._ensure_log().addHandler(handler)

    @classmethod
    def init_arg(cls, args=None):
        if not args:
            return
        # detect ArgumentParser without importing argparse eagerly
        from argparse import ArgumentParser
        if isinstance(args, ArgumentParser):
            _args, _ = args.parse_known_args()
            if hasattr(_args, "config") and _args.config and isfile(_args.config):
                cls.add_config(_args.config)
        elif hasattr(args, "config") and args.config and isfile(args.config):
            cls.add_config(args.config)
        cls._args = args
        cls._ensure_conf().update_args(args)

    @classmethod
    def init_log(cls, logfile=None, name=__package__, level="info"):
        cls._log = getlog(logfile=logfile, level=level, name=name)
        conf = cls._ensure_conf()
        if conf.args.get("debug"):
            cls._log.setLevel(logging.DEBUG)
        if conf.args.get("quiet"):
            cls._log.setLevel(logging.CRITICAL)

    @classmethod
    def init_bin(cls):
        cls._ensure_conf().update_executable_bin()

    @classmethod
    def init_all(cls, args=None, conf=None, init_bin=False, app=__package__):
        cls._conf = load_config(app=app)
        cls.init_arg(args)
        cls.init_log(name=app)
        cls.add_path()
        cls.add_config(conf)
        if init_bin:
            cls.init_bin()

    Initial = init_all

    @classmethod
    def init(cls, *cf, init_bin=False, args=None, app=__package__, **kw):
        cls(*cf, init_bin=init_bin, args=args, app=app, **kw)

    # ── attribute delegation ────────────────────────────────────────

    def __getattr__(self, attr):
        """Delegate unknown attributes to conf.

        Private / dunder attrs (``_*``, ``__*__``) are resolved via
        ``__dict__`` only — they never fall through to config lookup.
        """
        try:
            return self.__dict__[attr]
        except KeyError:
            if attr.startswith('_'):
                raise AttributeError(attr)
            return self.conf[attr]

    def __setattr__(self, key, value):
        self.conf.__setitem__(key, value)

    __getitem__ = __getattr__
    __setitem__ = __setattr__


# singleton convenience instance
context = Context()


class debug(object):
    """Decorator / descriptor that temporarily elevates the context logger
    to DEBUG level for the duration of the wrapped call."""

    def __init__(self, func):
        wraps(func)(self)

    def __call__(self, *args, **kwargs):
        logger = context.log
        prev_level = logger.level
        try:
            logger.setLevel(logging.DEBUG)
            return self.__wrapped__(*args, **kwargs)
        finally:
            logger.setLevel(prev_level)

    def __get__(self, instance, cls):
        if instance is None:
            return self
        return types.MethodType(self, instance)
