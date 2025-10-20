import os
import io
import sys
import pdb
import json
import argparse
import importlib
import configparser

from os.path import isfile, exists, join, abspath, realpath, split, expanduser, dirname

from .utils import (
    Iterable,
    total_ordering,
    user_config_dir,
    which,
    is_exe,
    load_it,
    option_on_command_line,
    USER_CONF_FILE,
    PKG_CONF_FILE,
    CONF_FILE_NAME,
)


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

    def __getstate__(self):
        return list(self.__dict__.items())

    def __setstate__(self, items):
        for key, val in items:
            self.__dict__[key] = val


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


class JsonEncoder(json.JSONEncoder):

    def default(self, field):
        try:
            serializa = json.JSONEncoder.default(self, field)
        except TypeError:
            return str(field)


@total_ordering
class configValue(object):

    def __init__(self, value, src=None):
        if isinstance(value, configValue):
            self._value = value._value
        else:
            self._value = value
        self._src = src

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self._value.__repr__())

    __str__ = __repr__

    def __eq__(self, other):
        return isinstance(other, configValue) and self._value == other._value or self._value == other

    def __lt__(self, other):
        return isinstance(other, configValue) and self._value < other._value or self._value < other

    def __hash__(self):
        return hash(self._value)

    def __getattr__(self, attr):
        return getattr(self._value, attr)

    def __getitem__(self, item):
        return self._value[item]

    def __setitem__(self, key, value):
        self._value[key] = value

    def __delitem__(self, key):
        del self._value[key]

    def __contains__(self, item):
        return item in self._value


class Config(Dict):

    def __init__(self, config_file=None, init_bin=False, bin_dir=None, **kw):
        '''
        The `config_file` argument must be file-path or iterable. If `config_file` is iterable, returning one line at a time, and `name` attribute must be needed for file path.
        Return `Config` object
        '''
        super(Config, self).__init__()
        self._cf = []
        self._command_line_options = {}
        self.bin = self.soft = self.software
        self.database = self.db
        self._bin_dirs = bin_dir and [bin_dir, ] or [join(sys.prefix, "bin"), ]
        if init_bin or kw.get("init_envs") or kw.get("init_env"):
            self.update_executable_bin()
        if config_file is None:
            return
        if not isinstance(config_file, (str, bytes)) and isinstance(config_file, Iterable) and hasattr(config_file, "name"):
            self._path = abspath(config_file.name)
            self._cf.append(self._path)
            self.__config = Conf()
            self.__config.read_file(config_file)
        else:
            self._path = abspath(config_file)
            self._cf.append(self._path)
            if not isfile(self._path):
                return
            self.__config = Conf()
            self.__config.read(self._path)
        for s in self.__config.sections():
            for k, v in self.__config[s].items():
                self[s][k] = load_it(v)

    def rget(self, key, *keys, default=None):
        '''default value: None'''
        v = self[key]
        for k in keys:
            try:
                v = v.get(k, default)
            except AttributeError as e:
                raise KeyError(k)
        return v

    def __get_conf_parser(self, config):
        if not isinstance(config, (str, bytes)) and isinstance(config, Iterable) and hasattr(config, "name"):
            self._cf.append(abspath(config.name))
            c = Conf()
            c.read_file(config)
        else:
            self._cf.append(abspath(config))
            if not isfile(config):
                return
            c = Conf()
            c.read(config)
        return c

    def update_config(self, config, override=True):
        parser = self.__get_conf_parser(config)
        if parser:
            for s in parser.sections():
                d = self._getvalue(s)
                for k, v in parser[s].items():
                    if k not in d or v != "" and (override or d[k] == ""):
                        if v != "":
                            d[k] = load_it(v)

    def add_config(self, config):
        self.update_config(config, override=False)

    def update_dict(self, args=None, **kwargs):
        if args and isinstance(args, argparse.ArgumentParser):  # parser
            _args, _ = args.parse_known_args()
            prefix_char = args.prefix_chars
            for action in args._actions:
                if action.dest not in _args:
                    continue
                self._command_line_options[action.dest] = option_on_command_line(
                    prefix_chars=prefix_char, option_strings=action.option_strings)
            for k, v in _args.__dict__.items():
                if self._command_line_options.get(k) or k not in self["args"] or self["args"][k] == "":
                    self["args"][k] = v
        elif args and hasattr(args, "__dict__"):  # args NameSpace
            for k, v in args.__dict__.items():
                self["args"][k] = v
        for k, v in kwargs.items():
            self["args"][k] = v

    def update_args(self, args=None):
        self.update_dict(args)

    def print_config(self):
        private_items = ("_command_line_options", "_bin_dirs", "_cf")
        print("Configuration files to search (order by order):")
        for cf in self.search_order:
            print(f" - {abspath(cf)}")
        print("\nAvailable Config:")
        keyness_values = []
        for k, info in sorted(self.to_dict().items()):
            if k in private_items:
                continue
            elif not isinstance(info, dict):
                keyness_values.append((k, info))
                continue
            else:
                print(f"[{k}]")
                for v, p in sorted(info.items()):
                    if isinstance(p, dict) and not p:
                        p = None
                    if "secret" in v:
                        try:
                            p = hide_key(p)
                        except:
                            pass
                    print(f" - {v} : {p}")
        if keyness_values:
            print("[*]")
            for k, info in keyness_values:
                print(f" - {k}: {info}")

    @classmethod
    def load(cls, *config_files, app=None, **kwargs):
        '''
        @config_files: search config by config_files orders
        @app: search config in app_config_dir if no config_files define
        @init_bin <bool>: default: Fasle, this will add '{sys.prefix}/bin/' to 'Config.soft' if set True
        '''
        cfs = []
        if config_files:  # ignore app config
            cfs.extend(config_files[::-1])
        elif app:
            spec = importlib.util.find_spec(app)
            if spec:
                cfs.append(join(dirname(spec.origin), CONF_FILE_NAME))
            cfs.append(join(user_config_dir(app=app), CONF_FILE_NAME))
        else:
            cfs.extend([PKG_CONF_FILE, USER_CONF_FILE])
        conf = cls(config_file=None, **kwargs)
        for cf in cfs:
            conf.update_config(cf)
        return conf

    def update_executable_bin(self):
        '''
        only directory join(sys.prefix, "bin") 
        '''
        for bin_dir in self._bin_dirs:
            for bin_path in os.listdir(bin_dir):
                exe_path = join(bin_dir, bin_path)
                if is_exe(exe_path):
                    bin_key = bin_path.replace("-", "").replace("_", "")
                    if not self.rget("software", bin_key):
                        self["software"][bin_key] = exe_path

    @property
    def search_order(self):
        search_cf = self._cf[::-1]
        return sorted(set(search_cf), key=search_cf.index)

    def to_json(self, *args, **kw):
        return json.dumps(self, *args, cls=JsonEncoder, **kw)

    def to_dict(self):
        d = json.loads(self.to_json())
        return {k: v for k, v in d.items() if not (isinstance(v, dict) and not v)}

    def keys(self):
        return self.to_dict().keys()

    def values(self):
        return self.to_dict().values()

    def __iter__(self):
        return self.keys()

    def items(self):
        _item = dict(super(Config, self).items())
        dump = json.loads(json.dumps(_item, cls=JsonEncoder))
        d = {k: v for k, v in dump.items() if not (
            isinstance(v, dict) and not v)}
        return d.items()

    def __contains__(self, item):
        return item in self.to_dict()

    def __getitem__(self, name):
        res = self._getvalue(name)
        if res or not isinstance(res, (dict, Dict)):
            return res
        values = Dict()
        for k, v in self.items():
            if name in v and (k == "args" or v.get(name) != ""):
                values[k] = v.get(name)
        if not len(values):
            return res
        if len(values) == 1 or len(set(values.values())) == 1:
            return list(values.values())[0]
        if name in self._command_line_options:  # args value
            if self._command_line_options[name]:  # args command line value
                return values["args"]
            if "args" in values:  # args default value
                values.pop("args")
                if len(values) == 1 or len(set(values.values())) == 1:
                    return list(values.values())[0]
                else:
                    return values
        return values.get("args", values)  # args value first

    __getattr__ = __getitem__

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.to_dict())

    __str__ = __repr__

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()


load_config = Config.load


def hide_key(s):
    if len(s) > 6:
        return "%s******%s" % (s[:3], s[-3:])
    elif len(s) > 1:
        return "%s*****" % s[:1]
    else:
        return "******"
