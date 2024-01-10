import os
import sys
import configparser

from copy import copy
from os.path import isfile, exists, join, abspath, realpath, split, expanduser, dirname

from .utils import user_config_dir, which, is_exe, USER_CONF_FILE, PKG_CONF_FILE, CONF_FILE_NAME


if sys.version_info[0] == 3:
    from collections.abc import Iterable
else:
    from collections import Iterable


class Conf(configparser.ConfigParser):

    def optionxform(self, optionstr):
        return optionstr


class AttrDict(dict):

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self))

    def __setitem__(self, key, value):
        return super(AttrDict, self).__setitem__(key, value)

    def __getitem__(self, name):
        try:
            return super(AttrDict, self).__getitem__(name)
        except (KeyError, RecursionError):
            raise AttributeError(name)

    def __delitem__(self, name):
        return super(AttrDict, self).__delitem__(name)

    __getattr__ = __getitem__

    __setattr__ = __setitem__

    __delattr__ = __delitem__

    def copy(self):
        return self.__class__(self)


class Dict(AttrDict):
    '''A dictionary with attribute-style access. It maps attribute access to
    the real dictionary. Returns a `which(entry)` if key is not found. '''

    def __init__(self, *args, **kwargs):
        super(Dict, self).__init__(*args, **kwargs)
        self.__dict__["_default"] = which

    def __repr__(self):
        return "%s(%s, %r)" % (self.__class__.__name__, dict.__repr__(self),
                               self.__dict__["_default"])

    def __getitem__(self, name):
        try:
            return super(Dict, self).__getitem__(name)
        except (KeyError, AttributeError):
            df = self.__dict__["_default"](name)
            if df is not None:
                return df
            d = Dict()
            super(Dict, self).__setitem__(name, d)
            return d

    __getattr__ = __getitem__

    def _getvalue(self, name):
        try:
            return super(Dict, self).__getitem__(name)
        except (KeyError, AttributeError):
            d = Dict()
            super(Dict, self).__setitem__(name, d)
            return d


class ConfigType(type):

    _instance = None

    def __init__(self, *args, **kwargs):
        super(ConfigType, self).__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if self._instance is None:
            self._instance = super(ConfigType, self).__call__(*args, **kwargs)
        return self._instance

    def __getattr__(cls, attr):
        return cls.__dict__.get(attr, cls.conf.__getitem__(attr))

    __getitem__ = __getattr__


class Config(Dict):

    def __init__(self, config_file=None, init_bin=False, bin_dir=None, **kw):
        '''
        The `config_file` argument must be file-path or iterable. If `config_file` is iterable, returning one line at a time, and `name` attribute must be needed for file path.
        Return `Config` object
        '''
        super(Config, self).__init__()
        self.info = self
        self.cf = []
        self.bin = self.soft = self.software
        self.database = self.db
        self.bin_dirs = bin_dir and [bin_dir, ] or [join(sys.prefix, "bin"), ]
        if init_bin or kw.get("init_envs") or kw.get("init_env"):
            self.update_executable_bin()
        if config_file is None:
            return
        if not isinstance(config_file, (str, bytes)) and isinstance(config_file, Iterable) and hasattr(config_file, "name"):
            self._path = abspath(config_file.name)
            self.cf.append(self._path)
            self.__config = Conf()
            self.__config.read_file(config_file)
        else:
            self._path = abspath(config_file)
            self.cf.append(self._path)
            if not isfile(self._path):
                return
            self.__config = Conf()
            self.__config.read(self._path)
        for s in self.__config.sections():
            for k, v in self.__config[s].items():
                self[s][k] = v

    def rget(self, key, *keys, default=None):
        '''default value: None'''
        v = self[key]
        for k in keys:
            try:
                v = v.get(k, default)
            except AttributeError as e:
                raise KeyError(k)
        return v

    def update_config(self, config):
        if not isinstance(config, (str, bytes)) and isinstance(config, Iterable) and hasattr(config, "name"):
            self.cf.append(abspath(config.name))
            c = Conf()
            c.read_file(config)
        else:
            self.cf.append(abspath(config))
            if not isfile(config):
                return
            c = Conf()
            c.read(config)
        for s in c.sections():
            self[s].update(dict(c[s].items()))

    def update_dict(self, args=None, **kwargs):
        if args and hasattr(args, "__dict__"):
            self["args"].update(args.__dict__)
        self["args"].update(kwargs)

    def update_args(self, args=None):
        self.update_dict(args)

    def write_config(self, configile):
        with open(configile, "w") as fo:
            for s, info in self.items():
                if not info or type(info) != Dict:
                    continue
                if fo.tell():
                    fo.write("\n")
                fo.write("[%s]\n" % s)
                for k, v in info.items():
                    fo.write("%s = %s\n" % (k, v))

    def print_config(self):
        print("Configuration files to search (order by order):")
        for cf in self.search_order:
            print(" - %s" % abspath(cf))
        print("\nAvailable Config:")
        for k, info in sorted(self.items()):
            if not info or type(info) != Dict:
                continue
            print("[%s]" % k)
            for v, p in sorted(info.items()):
                if "secret" in v:
                    try:
                        p = hide_key(p)
                    except:
                        pass
                print(" - %s : %s" % (v, p))

    def update_executable_bin(self):
        '''
        only directory join(sys.prefix, "bin") 
        '''
        for bin_dir in self.bin_dirs:
            for bin_path in os.listdir(bin_dir):
                exe_path = join(bin_dir, bin_path)
                if is_exe(exe_path):
                    bin_key = bin_path.replace("-", "").replace("_", "")
                    if not self.rget("software", bin_key):
                        self["software"][bin_key] = exe_path

    @property
    def search_order(self):
        return self.cf[::-1]

    def copy(self):
        c = copy(self)
        c.info = c
        return c

    def __getitem__(self, name):
        res = self._getvalue(name)
        if type(res) != Dict or res:
            return res
        values = Dict()
        for k, v in self.items():
            if type(v) != Dict:
                continue
            if v.get(name):
                values[k] = v.get(name)
        if not values:
            return res
        if len(values) == 1 or len(set(values.values())) == 1:
            return list(values.values())[0]
        return values.args or values  # args first

    __getattr__ = __getitem__


def load_config(*args, **kwargs):
    '''
    @config_files: search config by args orders
    @init_bin <bool>: default: Fasle, this will add 'sys.prefix/bin/' to 'Config.soft' if set True
    '''
    cfs = args and args[::-1] or [PKG_CONF_FILE, USER_CONF_FILE]
    conf = Config(config_file=None, **kwargs)
    for cf in cfs:
        conf.update_config(cf)
    return conf


def print_config(conf):
    conf.print_config()


def hide_key(s):
    if len(s) > 6:
        return "%s******%s" % (s[:3], s[-3:])
    elif len(s) > 1:
        return "%s*****" % s[:1]
    else:
        return "******"
