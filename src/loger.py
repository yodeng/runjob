import logging


class Logger(object):

    def __init__(self):
        self.logs = {
            "stdout":  logging.getLogger("stdout"),
            "stderr":  logging.getLogger("stderr"),
            "default": logging.getLogger("default"),
        }
        self.set_logger(level=logging.INFO)

    def get_logger(self, name="default"):
        logger = self.logs[name]
        return logger

    def set_logger(self, level=logging.INFO, name="all"):
        names = self.logs.keys() if name == "all" else [name, ]
        for log_name in names:
            logger = self.get_logger(log_name)
            logger.setLevel(level)
            handler = logging.StreamHandler()
            handler.setFormatter(Formatter())
            logger.handlers = [handler, ]

    def diasble_logger(self, name="all"):
        names = self.logs.keys() if name == "all" else [name, ]
        for log_name in names:
            self.set_logger(logging.CRITICAL, log_name)


class Formatter(logging.Formatter):

    f_color_map = {'black': 30, 'red': 31, 'green': 32, 'yellow': 33,
                   'blue': 34, 'purple': 35, 'cyan': 36, 'white': 37, 'gray': 38}
    b_color_map = {'black': 40, 'red': 41, 'green': 42, 'yellow': 43,
                   'blue': 44, 'purple': 45, 'cyan': 46, 'white': 47, 'gray': 48}
    mode_map = {'bold': 1, 'dark': 2, 'underline': 4,
                'blink': 5, 'reverse': 7, 'concealed': 8}
    reset = '\x1b[0m'
    fmt = "[%(levelname)s %(asctime)s] %(message)s"
    fmt_debug = "[%(levelname)s %(threadName)s %(asctime)s module:%(module)s func:%(funcName)s (line:%(lineno)d)] %(message)s"

    def __init__(self, *args, **kw):
        super(Formatter, self).__init__(*args, **kw)
        self._formats = {
            logging.DEBUG: {
                "fmt": self.fmt_debug,
                "color": self._color(0, 36)  # cyan
            },
            logging.INFO: {
                "fmt": self.fmt,
                "color": self._color(0, 32)  # green
            },
            logging.WARNING: {
                "fmt": self.fmt,
                "color": self._color(0, 33)  # yellow
            },
            logging.ERROR: {
                "fmt": self.fmt,
                "color": self._color(0, 31)  # red
            },
            logging.CRITICAL: {
                "fmt": self.fmt,
                "color": self._color(0, 35)  # purple
            },
        }

    def _color(self, mode=0, fore=30):
        return '\x1b[%sm\x1b[%sm' % (mode, fore)

    def format(self, record):
        fmt = self._formats.get(record.levelno)
        log_fmt = fmt["color"] + fmt["fmt"] + self.reset
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
