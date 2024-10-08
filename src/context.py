import os
import sys
import types
import logging

from functools import wraps
from os.path import isfile, isdir, join, dirname, abspath

from .utils import getlog
from .config import ConfigType, load_config


class Context(metaclass=ConfigType):

    conf = load_config()
    db = database = conf.database
    soft = bin = software = conf.software
    args = conf.args
    _args = None
    log = getlog()

    def __init__(self, *cf, init_bin=False, args=None, app=__package__, **kw):
        self.__class__.conf = load_config(
            *cf, init_bin=init_bin or kw.get("init_envs"), app=app)
        if cf or init_bin or kw.get("init_envs"):
            self.__class__.args = self.__class__.conf.args
            self.__class__.db = self.__class__.database = self.__class__.conf.database
            self.__class__.soft = self.__class__.bin = self.__class__.software = self.__class__.conf.software
        self.__class__.init_arg(args)
        self.__class__.init_log(name=app)

    @classmethod
    def add_config(cls, config=None):
        if config:
            cls.conf.update_config(config)

    @classmethod
    def add_bin(cls, bin_dir=None):
        if bin_dir:
            cls.conf.bin_dirs.append(bin_dir)
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
    def init_arg(cls, args=None):
        if not args:
            return
        if hasattr(args, "config") and args.config and isfile(args.config):
            cls.add_config(args.config)
        cls._args = args
        cls.conf.update_args(args)

    @classmethod
    def init_log(cls, logfile=None, name=__package__, level="info"):
        cls.log = getlog(logfile=logfile, level=level, name=name)
        if cls.conf.args.get("debug"):
            cls.log.setLevel(logging.DEBUG)
        if cls.conf.args.get("quiet"):
            logging.disable()

    @classmethod
    def init_bin(cls):
        cls.conf.update_executable_bin()

    @classmethod
    def init_all(cls, args=None, conf=None, init_bin=False, app=__package__):
        cls.conf = load_config(app=app)
        cls.init_arg(args)
        cls.init_log(name=app)
        cls.add_path()
        cls.add_config(conf)
        if init_bin:
            cls.init_bin()

    Initial = init_all

    @classmethod
    def init(cls,  *cf, init_bin=False, args=None, app=__package__, **kw):
        cls(*cf, init_bin=init_bin, args=args, app=app, **kw)

    def __getattr__(self, attr):
        return self.__dict__.get(attr, self.conf.__getitem__(attr))

    def __setattr__(self, key, value):
        return self.__class__.conf.__setitem__(key, value)

    __getitem__ = __getattr__

    __setitem__ = __setattr__


context = Context


class debug(object):

    def __init__(self, func):
        wraps(func)(self)

    def __call__(self, *args, **kwargs):  # wrapper function
        level = context.log.level
        try:
            context.log.setLevel(logging.DEBUG)
            return self.__wrapped__(*args, **kwargs)
        finally:
            context.log.setLevel(level)

    def __get__(self, instance, cls):  # wrapper instance method
        if instance is None:
            return self
        return types.MethodType(self, instance)
