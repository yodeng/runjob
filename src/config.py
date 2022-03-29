import os
import configparser


class Conf(configparser.ConfigParser):
    def __init__(self, defaults=None):
        super(Conf, self).__init__(defaults=defaults)

    def optionxform(self, optionstr):
        return optionstr


class Dict(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value


class Config(object):

    def __init__(self, config_file=None):
        self.cf = []
        self.info = Dict()
        if config_file is None:
            return
        self._path = os.path.join(os.getcwd(), config_file)
        self.cf.append(self._path)
        if not os.path.isfile(self._path):
            return
        self._config = Conf()
        self._config.read(self._path)
        for s in self._config.sections():
            self.info[s] = Dict(dict(self._config[s].items()))

    def get(self, section, name):
        return self.info.get(section, Dict()).get(name, None)

    def update_config(self, config):
        self.cf.append(config)
        if not os.path.isfile(config):
            return
        c = Conf()
        c.read(config)
        for s in c.sections():
            self.info.setdefault(s, Dict()).update(dict(c[s].items()))

    def update_dict(self, **kwargs):
        self.info.setdefault("args", Dict()).update(kwargs)

    def write_config(self, configile):
        with open(configile, "w") as fo:
            for s, info in self.info.items():
                fo.write("[%s]\n" % s)
                for k, v in info.items():
                    fo.write("%s = %s\n" % (k, v))
                fo.write("\n")

    def __str__(self):
        return self.info

    def __call__(self):
        return self.info


def load_config():
    configfile_home = os.path.join(os.path.expanduser("~"), ".runjobconfig")
    configfile_default = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'runjobconfig')
    conf = Config(configfile_default)
    conf.update_config(configfile_home)
    return conf


def print_config(conf):
    print("Configuration files to search (order by order):")
    for cf in conf.cf[::-1]:
        print(" - %s" % os.path.abspath(cf))
    print("\nAvailable Config:")
    for k, info in conf.info.items():
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
