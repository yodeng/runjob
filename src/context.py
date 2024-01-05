import os
import sys
import logging

from os.path import isfile, join, dirname

from .utils import getlog
from .config import ConfigType, load_config


class Context(metaclass=ConfigType):

    conf = load_config()
    db = database = conf.database
    soft = bin = software = conf.software
    args = conf.args
    log = getlog()

    def __init__(self, home=None, default=None, init_envs=False):
        if home or default or init_envs:
            self.__class__.conf = load_config(
                home=home, default=default, init_envs=init_envs)
            self.__class__.args = self.__class__.conf.args
            self.__class__.db = self.__class__.database = self.__class__.conf.database
            self.__class__.soft = self.__class__.bin = self.__class__.software = self.__class__.conf.software

    @classmethod
    def _add_config(cls, config):
        cls.conf.update_config(config)

    @classmethod
    def _add_path(cls, path_dir=None):
        cls.conf.bin_dir = path_dir or cls.conf.bin_dir
        cls.conf.update_executable_bin()

    @classmethod
    def init_arg(cls, args=None):
        if not args:
            return
        if hasattr(args, "config") and args.config and isfile(args.config):
            cls._add_config(args.config)
        cls.conf.update_args(args)

    @classmethod
    def init_log(cls, logfile=None, name=__package__, level="info"):
        cls.log = getlog(logfile=logfile, level=level, name=name)
        if cls.conf.args.debug:
            cls.log.setLevel(logging.DEBUG)
        if cls.conf.args.quiet:
            logging.disable()

    @classmethod
    def init_path(cls):
        os.environ["PATH"] = join(sys.prefix, "bin:") + os.environ["PATH"]

    @classmethod
    def init_all(cls, args=None):
        cls.init_arg(args)
        cls.init_log()
        cls.init_path()

    def __getattr__(self, attr):
        return self.__dict__.get(attr, self.conf.__getitem__(attr))

    def __setattr__(self, key, value):
        return self.__class__.conf.__setitem__(key, value)

    __getitem__ = __getattr__

    __setitem__ = __setattr__


context = Context
