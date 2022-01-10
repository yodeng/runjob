import os
import configparser


class Conf(configparser.ConfigParser):
    def __init__(self, defaults=None):
        configparser.ConfigParser.__init__(self, defaults=None)

    def optionxform(self, optionstr):
        return optionstr


class Config(object):

    def __init__(self, config_file=None):
        self.cf = []
        self.info = {}
        self._path = os.path.join(os.getcwd(), config_file)
        self.cf.append(self._path)
        if not os.path.isfile(self._path):
            return
        self._config = Conf()
        self._config.read(self._path)
        for s in self._config.sections():
            self.info[s] = dict(self._config[s].items())

    def get(self, section, name):
        return self.info.get(section, {}).get(name, None)

    def update_config(self, config):
        self.cf.append(config)
        if not os.path.isfile(config):
            return
        c = Conf()
        c.read(config)
        for s in c.sections():
            self.info.setdefault(s, {}).update(dict(c[s].items()))

    def update_dict(self, **kwargs):
        self.info.setdefault("args", {}).update(kwargs)

    def __str__(self):
        print(self.info)


def load_config():
    configfile_home = os.path.join(os.path.expanduser("~"), ".runjob.ini")

    configfile_default = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'runjob.ini')

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
        for v, p in info.items():
            print(" - %s : %s" % (v, p))
