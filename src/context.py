import os
import sys
import logging

from os.path import isfile, isdir, join, dirname, abspath

from .utils import getlog
from .config import ConfigType, load_config


class Context(metaclass=ConfigType):

    conf = load_config()
    db = database = conf.database
    soft = bin = software = conf.software
    args = conf.args
    log = getlog()

    def __init__(self, *cf, init_bin=False, args=None, **kw):
        if cf or init_bin or kw.get("init_envs"):
            self.__class__.conf = load_config(
                *cf, init_bin=init_bin or kw.get("init_envs"))
            self.__class__.args = self.__class__.conf.args
            self.__class__.db = self.__class__.database = self.__class__.conf.database
            self.__class__.soft = self.__class__.bin = self.__class__.software = self.__class__.conf.software
        self.__class__.init_arg(args)

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
    def init_all(cls, args=None, conf=None, init_bin=False):
        cls.init_arg(args)
        cls.init_log()
        cls.add_path()
        cls.add_config(conf)
        if init_bin:
            cls.init_bin()

    @classmethod
    def init(cls, home=None, default=None, init_bin=False, args=None, **kw):
        cls(home=home, default=default, init_bin=init_bin, args=args, **kw)

    def __getattr__(self, attr):
        return self.__dict__.get(attr, self.conf.__getitem__(attr))

    def __setattr__(self, key, value):
        return self.__class__.conf.__setitem__(key, value)

    __getitem__ = __getattr__

    __setitem__ = __setattr__


context = Context
