import os
import sys
import configparser

from collections import defaultdict


class Conf(configparser.ConfigParser):

    def __init__(self, defaults=None):
        super(Conf, self).__init__(defaults=defaults)

    def optionxform(self, optionstr):
        return optionstr


class Dict(dict):

    def __getattr__(self, name):
        return self.get(name, which(name))

    def __setattr__(self, name, value=None):
        self[name] = value

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, data):
        return self.__init__(data)


class Defaultdict(defaultdict, Dict):

    def __getattr__(self, name):
        return defaultdict.__getitem__(self, name)


class ConfigType(type):

    _instance = None

    def __init__(self, *args, **kwargs):
        super(ConfigType, self).__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if self._instance is None:
            self._instance = super(ConfigType, self).__call__(*args, **kwargs)
        return self._instance


class Config(object):

    def __init__(self, config_file=None, init_envs=False):
        self.cf = []
        self.info = Defaultdict(Dict)
        self.info.bin = self.info.software
        self.info.database = self.info.db = self.info.data
        if init_envs:
            self.update_executable_bin()
        if config_file is None:
            return
        self._path = os.path.join(os.getcwd(), config_file)
        self.cf.append(self._path)
        if not os.path.isfile(self._path):
            return
        self._config = Conf()
        self._config.read(self._path)
        for s in self._config.sections():
            for k, v in self._config[s].items():
                self.info[s][k] = v

    def get(self, section, name):
        return self.info[section].get(name, None)

    def update_config(self, config):
        self.cf.append(config)
        if not os.path.isfile(config):
            return
        c = Conf()
        c.read(config)
        for s in c.sections():
            self.info[s].update(dict(c[s].items()))

    def update_dict(self, **kwargs):
        self.info["args"].update(kwargs)

    def write_config(self, configile):
        with open(configile, "w") as fo:
            for s, info in self.info.items():
                fo.write("[%s]\n" % s)
                for k, v in info.items():
                    fo.write("%s = %s\n" % (k, v))
                fo.write("\n")

    def update_executable_bin(self):
        bindir = os.path.abspath(os.path.dirname(sys.executable))
        for bin_path in os.listdir(bindir):
            exe_path = os.path.join(bindir, bin_path)
            if is_exe(exe_path):
                bin_key = bin_path.replace("-", "").replace("_", "")
                if not self.get("software", bin_key):
                    self.info["software"][bin_key] = exe_path

    def __str__(self):
        return self.info.__str__()

    __repr__ = __str__

    __metaclass__ = ConfigType

    def __call__(self):
        return self.info

    def __getattr__(self, name):
        if self.info.get(name) is not None:
            return self.info.get(name)
        values = Dict()
        for k, v in self.info.items():
            if v.get(name) is not None:
                values[k] = v.get(name)
        if not len(values):
            return
        if len(values) == 1 or len(set(values.values())) == 1:
            return list(values.values())[0]
        return values


def which(program, paths=None):
    ex = os.path.dirname(sys.executable)
    found_path = None
    fpath, fname = os.path.split(program)
    if fpath:
        program = canonicalize(program)
        if is_exe(program):
            found_path = program
    else:
        if is_exe(os.path.join(ex, program)):
            return os.path.join(ex, program)
        paths_to_search = []
        if isinstance(paths, (tuple, list)):
            paths_to_search.extend(paths)
        else:
            env_paths = os.environ.get("PATH", "").split(os.pathsep)
            paths_to_search.extend(env_paths)
        for path in paths_to_search:
            exe_file = os.path.join(canonicalize(path), program)
            if is_exe(exe_file):
                found_path = exe_file
                break
    return found_path


def is_exe(file_path):
    return (
        os.path.exists(file_path)
        and os.access(file_path, os.X_OK)
        and os.path.isfile(os.path.realpath(file_path))
    )


def canonicalize(path):
    return os.path.abspath(os.path.expanduser(path))


def load_config(home=None, default=None, **kwargs):
    '''
    @home <file>: default: ~/.runjobconfig, home config is priority then default config
    @default <file>: default: dirname(__file__)/runjobconfig
    @init_envs <bool>: default: Fasle, this will add sys.prefix/bin to PATH for cmd search if init_envs=True
    '''
    configfile_home = home or os.path.join(
        os.path.expanduser("~"), ".runjobconfig")
    configfile_default = default or os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'runjobconfig')
    conf = Config(config_file=configfile_default, **kwargs)
    conf.update_config(configfile_home)
    return conf


def print_config(conf):
    print("Configuration files to search (order by order):")
    for cf in conf.cf[::-1]:
        print(" - %s" % os.path.abspath(cf))
    print("\nAvailable Config:")
    for k, info in sorted(conf.info.items()):
        if not len(info):
            continue
        print("[%s]" % k)
        for v, p in sorted(info.items()):
            if "secret" in v:
                try:
                    p = hide_key(p)
                except:
                    pass
            print(" - %s : %s" % (v, p))


def hide_key(s):
    if len(s) > 6:
        return "%s******%s" % (s[:3], s[-3:])
    elif len(s) > 1:
        return "%s*****" % s[:1]
    else:
        return "******"
