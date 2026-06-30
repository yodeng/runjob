import sys
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
        return self.logs[name]

    def set_logger(self, level=logging.INFO, name="all"):
        names = self.logs.keys() if name == "all" else [name, ]
        handler = logging.StreamHandler()
        handler.setFormatter(Formatter())
        for log_name in names:
            logger = self.get_logger(log_name)
            logger.setLevel(level)
            logger.handlers = [handler, ]

    def disable_logger(self, name="all"):
        names = self.logs.keys() if name == "all" else [name, ]
        for log_name in names:
            self.set_logger(logging.CRITICAL, log_name)


class Formatter(logging.Formatter):
    """Logging formatter with ANSI color support.

    Color/mode maps are class attributes (shared across all instances).
    Sub-formatters are pre-created once in __init__ to avoid per-record overhead.
    """

    # ── class-level colour & style maps ────────────────────────────
    f_color_map = {'black': 30, 'red': 31, 'green': 32, 'yellow': 33,
                   'blue': 34, 'purple': 35, 'cyan': 36, 'white': 37, 'gray': 38}
    b_color_map = {'black': 40, 'red': 41, 'green': 42, 'yellow': 43,
                   'blue': 44, 'purple': 45, 'cyan': 46, 'white': 47, 'gray': 48}
    mode_map = {'bold': 1, 'dark': 2, 'underline': 4,
                'blink': 5, 'reverse': 7, 'concealed': 8}
    reset = '\x1b[0m'

    fmt = "[%(levelname)s %(asctime)s] %(message)s"
    fmt_debug = "[%(levelname)s %(threadName)s %(asctime)s module:%(module)s func:%(funcName)s (line:%(lineno)d)] %(message)s"

    # format-string cache keyed by levelno — built once per process
    _format_cache = {}

    def __init__(self, *args, **kw):
        super(Formatter, self).__init__(*args, **kw)
        # pre-create a logging.Formatter for every level so that format()
        # never allocates a closure / Formatter per record.
        if not Formatter._format_cache:
            Formatter._format_cache = {
                logging.DEBUG: logging.Formatter(
                    self._color(0, 36) + self.fmt_debug + self.reset),
                logging.INFO: logging.Formatter(
                    self._color(0, 32) + self.fmt + self.reset),
                logging.WARNING: logging.Formatter(
                    self._color(0, 33) + self.fmt + self.reset),
                logging.ERROR: logging.Formatter(
                    self._color(0, 31) + self.fmt + self.reset),
                logging.CRITICAL: logging.Formatter(
                    self._color(0, 35) + self.fmt + self.reset),
            }

    @staticmethod
    def _color(mode=0, fore=30):
        return f'\x1b[{mode}m\x1b[{fore}m'

    def format(self, record):
        formatter = self._format_cache.get(record.levelno)
        if formatter is not None:
            return formatter.format(record)
        # fallback for unknown levels — still cheaper than the old per-call alloc
        log_fmt = self._color(0, 37) + self.fmt + self.reset
        return logging.Formatter(log_fmt).format(record)


def getlog(logfile=None, level="info", name=__package__):
    logger = logging.getLogger(name)
    if level.lower() == "info":
        logger.setLevel(logging.INFO)
    elif level.lower() == "debug":
        logger.setLevel(logging.DEBUG)
    if logfile is None:
        if logger.hasHandlers():
            return logger
        h = logging.StreamHandler(sys.stdout)
    else:
        h = logging.FileHandler(logfile, mode='a')
    h.setFormatter(Formatter())
    logger.addHandler(h)
    return logger
