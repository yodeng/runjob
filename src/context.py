import os
import sys
import logging

from os.path import isfile, join, dirname

from .utils import getlog
from .config import ConfigType, load_config


class Context(metaclass=ConfigType):

    def __init__(self, home=None, default=None, init_envs=False):
        self.conf = load_config(
            home=home, default=default, init_envs=init_envs)
        self.db = self.database = self.conf.database
        self.soft = self.bin = self.conf.bin
        self.args = self.conf.args

    def _add_config(self, config):
        self.conf.update_config(config)

    def _add_path(self, path_dir=None):
        self.conf.bin_dir = path_dir or self.conf.bin_dir
        self.conf.update_executable_bin()

    def init_arg(self, args=None):
        if not args:
            return
        if hasattr(args, "config") and args.config and isfile(args.config):
            self._add_config(args.config)
        self.conf.update_dict(**args.__dict__)

    def init_log(self, name=__package__):
        self.log = getlog(level="info", name=name)
        if self.args.debug:
            self.log.setLevel(logging.DEBUG)
        if self.args.quiet:
            logging.disable()

    def init_path(self):
        os.environ["PATH"] = join(sys.prefix, "bin:") + os.environ["PATH"]

    def init_all(self, args=None):
        self.init_arg(args)
        self.init_log()
        self.init_path()

    def __getattr__(self, attr):
        return self.__dict__.get(attr, getattr(self.conf, attr))

    __getitem__ = __getattr__
