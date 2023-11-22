import os
import sys
import configparser

from copy import copy
from os.path import isfile, exists, join, abspath, realpath, split, expanduser, dirname

if sys.version_info[0] == 3:
    from collections.abc import Iterable
else:
    from collections import Iterable


class Conf(configparser.ConfigParser):

    def __init__(self, defaults=None):
        super(Conf, self).__init__(defaults=defaults)

    def optionxform(self, optionstr):
        return optionstr


class AttrDict(dict):
    '''A dictionary with attribute-style access. It maps attribute access to
    the real dictionary. '''

    def __getstate__(self):
        return self.__dict__.items()

    def __setstate__(self, items):
        for key, val in items:
            self.__dict__[key] = val

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self))

    def __setitem__(self, key, value):
        return super(AttrDict, self).__setitem__(key, value)

    def __getitem__(self, name):
        return super(AttrDict, self).__getitem__(name)

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
        except KeyError:
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
        except KeyError:
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


class Config(Dict):

    def __init__(self, config_file=None, init_envs=False, bin_dir=None):
        '''
        The `config_file` argument must be file-path or iterable. If `config_file` is iterable, returning one line at a time, and `name` attribute must be needed for file path.
        Return `Config` object
        '''
        super(Config, self).__init__()
        self.info = self
        self.cf = []
        self.bin = self.software
        self.database = self.db = self.data
        self.bin_dir = bin_dir or join(sys.prefix, "bin")
        if init_envs:
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
                fo.write("[%s]\n" % s)
                for k, v in info.items():
                    fo.write("%s = %s\n" % (k, v))
                fo.write("\n")

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
        for bin_path in os.listdir(self.bin_dir):
            exe_path = join(self.bin_dir, bin_path)
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


def which(program, paths=None):
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
    return (
        exists(file_path)
        and os.access(file_path, os.X_OK)
        and isfile(realpath(file_path))
    )


def canonicalize(path):
    return abspath(expanduser(path))


def load_config(home=None, default=None, **kwargs):
    '''
    @home <file>: default: ~/.runjobconfig, home config is priority then default config
    @default <file>: default: dirname(__file__)/runjobconfig
    @init_envs <bool>: default: Fasle, this will add sys.prefix/bin to PATH for cmd search if init_envs=True
    '''
    configfile_home = home or join(
        expanduser("~"), ".runjobconfig")
    configfile_default = default or join(dirname(
        abspath(__file__)), 'runjobconfig')
    conf = Config(config_file=configfile_default, **kwargs)
    conf.update_config(configfile_home)
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
