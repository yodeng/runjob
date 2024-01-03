import io
import os
import sys
import argparse

from .utils import *
from ._version import __version__
from .config import load_config, print_config

try:
    from rich_argparse import RichHelpFormatter as HelpFormatter
except ImportError:
    from argparse import HelpFormatter


class CustomHelpFormatter(HelpFormatter):

    def _get_help_string(self, action):
        """Place default and required value in help string."""
        h = action.help

        # Remove any formatting used for Sphinx argparse hints.
        h = h.replace('``', '')

        if '%(default)' not in action.help and "default:" not in action.help:
            if action.default != '' and action.default != [] and \
                    action.default is not None and \
                    not isinstance(action.default, bool) and \
                    not isinstance(action.default, io.IOBase):
                if action.default is not argparse.SUPPRESS:
                    defaulting_nargs = [
                        argparse.OPTIONAL, argparse.ZERO_OR_MORE]

                    if action.option_strings or action.nargs in defaulting_nargs:
                        if '\n' in h:
                            lines = h.splitlines()
                            lines[0] += ' (default: %(default)s)'
                            h = '\n'.join(lines)
                        else:
                            h += ' (default: %(default)s)'
        if "required" not in action.help and hasattr(action, "required") and action.required:
            h += " " + REQUIRED
        return h

    def _format_action_invocation(self, action):
        """Removes duplicate ALLCAPS with positional arguments."""
        if not action.option_strings:
            default = self._get_default_metavar_for_positional(action)
            metavar, = self._metavar_formatter(action, default)(1)
            return metavar

        else:
            parts = []

            # if the Optional doesn't take a value, format is:
            #    -s, --long
            if action.nargs == 0:
                parts.extend(action.option_strings)

            # if the Optional takes a value, format is:
            #    -s ARGS, --long ARGS
            else:
                default = self._get_default_metavar_for_optional(action)
                args_string = self._format_args(action, default)
                for option_string in action.option_strings:
                    parts.append(option_string)

                return '%s %s' % (', '.join(parts), args_string)

            return ', '.join(parts)

    def _get_default_metavar_for_optional(self, action):
        return action.dest.upper()

    def _get_default_metavar_for_positional(self, action):
        return action.dest


def show_help_on_empty_command():
    if len(sys.argv) == 1:
        sys.argv.append('--help')


def color_description(parser):
    parser.description = style(
        parser.description, fore="red", mode="underline")


def default_parser():
    p = argparse.ArgumentParser(add_help=False)
    base = p.add_argument_group("base arguments")
    base.add_argument('-v', '--version',
                      action='version', version="v" + __version__)
    base.add_argument("-j", "--jobfile", type=argparse.FileType('r'), nargs="?", default=sys.stdin,
                      help="input jobfile, if empty, stdin is used. " + REQUIRED, metavar="<jobfile>")
    base.add_argument("-n", "--num", type=int,
                      help="the max job number runing at the same time. (default: all of the jobfile, max 1000)", metavar="<int>")
    base.add_argument("-s", "--start", type=int,
                      help="which line number(1-base) be used for the first job. (default: %(default)s)", metavar="<int>", default=1)
    base.add_argument("-e", "--end", type=int,
                      help="which line number (include) be used for the last job. (default: last line of the jobfile)", metavar="<int>")
    base.add_argument("-w", "--workdir", type=str, help="work directory.",
                      default=abspath(os.getcwd()), metavar="<workdir>")
    base.add_argument('-d', '--debug', action='store_true',
                      help='log debug info.', default=False)
    base.add_argument("-l", "--log", type=str,
                      help='append log info to file. (default: stdout)', metavar="<file>")
    base.add_argument('-r', '--retry', help="retry N times of the error job, 0 or minus means do not re-submit.",
                      type=int, default=0, metavar="<int>")
    base.add_argument('-R', '--retry-sec', help="retry the error job after N seconds.",
                      type=int, default=2, metavar="<int>")
    base.add_argument("-f", "--force", default=False, action="store_true",
                      help="force to submit jobs even if already successed.")
    base.add_argument("--dot", action="store_true", default=False,
                      help="do not execute anything and print the directed acyclic graph of jobs in the dot language.")
    base.add_argument("--dot-shrinked", action="store_true", default=False,
                      help="do not execute anything and print the shrinked directed acyclic graph of jobs in the dot language.")
    base.add_argument('--mode', type=str, default="sge", choices=["sge", "local", "localhost", "batchcompute"],
                      help="the mode to submit your jobs, if no sge installed, always localhost.")
    base.add_argument("--local", default=False, action="store_true",
                      help="submit your jobs in localhost, same as '--mode local'.")
    base.add_argument("--strict", action="store_true", default=False,
                      help="use strict to run, means if any errors, clean all jobs and exit.")
    base.add_argument("--quiet", action="store_true", default=False,
                      help="suppress all output and logging.")
    base.add_argument('--ini', metavar="<configfile>",
                      help="input configfile for configurations search.")
    base.add_argument('--config',  action='store_true', default=False,
                      help="show configurations and exit.")
    base.add_argument('--max-check', help="maximal number of job status checks per second, fractions allowed.",
                      type=float, default=DEFAULT_MAX_CHECK_PER_SEC, metavar="<float>")
    base.add_argument('--max-submit', help="maximal number of jobs submited per second, fractions allowed.",
                      type=float, default=DEFAULT_MAX_SUBMIT_PER_SEC, metavar="<float>")
    timeout_parser(p)
    return p


def timeout_parser(parser):
    time_args = parser.add_argument_group("time control arguments")
    time_args.add_argument('--max-queue-time', help="maximal time (d/h/m/s) between submit and running per job. (default: no-limiting)",
                           type=str, default=sys.maxsize, metavar="<float/str>")
    time_args.add_argument('--max-run-time', help="maximal time (d/h/m/s) start from running per job. (default: no-limiting)",
                           type=str, default=sys.maxsize, metavar="<float/str>")
    time_args.add_argument('--max-wait-time', help="maximal time (d/h/m/s) start from submit per job. (default: no-limiting)",
                           type=str, default=sys.maxsize, metavar="<float/str>")
    time_args.add_argument('--max-timeout-retry', help="retry N times for the timeout error job, 0 or minus means do not re-submit.",
                           type=int, default=0, metavar="<int>")


def sge_parser(parser):
    sge = parser.add_argument_group("sge arguments")
    sge.add_argument("-q", "--queue", type=str, help="the queue your job running, multi queue can be sepreated by whitespace. (default: all accessed queue)",
                     nargs="*", metavar="<queue>")
    sge.add_argument("-m", "--memory", type=int,
                     help="the memory used per command (GB).", default=1, metavar="<int>")
    sge.add_argument("-c", "--cpu", type=int,
                     help="the cpu numbers you job used.", default=1, metavar="<int>")


def batchcmp_parser(parser):
    batchcmp = parser.add_argument_group("batchcompute arguments")
    batchcmp.add_argument("--out-maping", type=str,
                          help='the oss output directory if your mode is "batchcompute", all output file will be mapping to you OSS://BUCKET-NAME. if not set, any output will be reserved.', metavar="<dir>")
    batchcmp.add_argument('--access-key-id', type=str,
                          help="AccessKeyID while access oss.", metavar="<str>")
    batchcmp.add_argument('--access-key-secret', type=str,
                          help="AccessKeySecret while access oss.", metavar="<str>")
    batchcmp.add_argument('--region', type=str, default="beijing", choices=['beijing', 'hangzhou', 'huhehaote', 'shanghai',
                                                                            'zhangjiakou', 'chengdu', 'hongkong', 'qingdao', 'shenzhen'], help="batch compute region.")


def runjob_parser():
    parser = argparse.ArgumentParser(
        description="%(prog)s is a tool for managing parallel tasks from a specific shell scripts runing in localhost, sge or batchcompute.",
        parents=[default_parser()],
        formatter_class=CustomHelpFormatter,
        allow_abbrev=False)
    parser.add_argument("-N", "--jobname", type=str,
                        help="job name. (default: basename of the jobfile)", metavar="<jobname>")
    parser.add_argument("-L", "--logdir", type=str,
                        help='the output log dir. (default: "%s/%s_*_log_dir")' % (os.getcwd(), "%(prog)s"), metavar="<logdir>")
    parser.add_argument("-g", "--groups", type=int, default=1,
                        help="N lines to consume a new job group.", metavar="<int>")
    parser.add_argument('--init', help="command before all jobs, will be running in localhost.",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--call-back', help="command after all jobs finished, will be running in localhost.",
                        type=str,  metavar="<cmd>")
    sge_parser(parser)
    batchcmp_parser(parser)
    color_description(parser)
    return parser


def runflow_parser():
    parser = argparse.ArgumentParser(
        description="%(prog)s is a tool for managing parallel tasks from a specific job file running in localhost or sge cluster.",
        parents=[default_parser()],
        formatter_class=CustomHelpFormatter,
        allow_abbrev=False)
    parser.add_argument('-i', '--injname', help="job names you need to run. (default: all job names of the jobfile)",
                        nargs="*", type=str, metavar="<str>")
    parser.add_argument("-L", "--logdir", type=str,
                        help='the output log dir. (default: join(dirname(jobfile), "logs"))', metavar="<logdir>")
    color_description(parser)
    return parser


def shell_job_parser(arglist):
    parser = runjob_parser()
    return parser.parse_known_args(arglist)[0]


def server_parser():
    parser = argparse.ArgumentParser(
        description="job status server (file socket).",
        formatter_class=CustomHelpFormatter)
    parser.add_argument("-f", "--file", type=str, help="socket file.",
                        metavar="<file>")
    parser.add_argument("-H", "--host", type=str, help="runjob server hostname or ip.",
                        metavar="<str>")
    parser.add_argument("-P", "--port", type=int, help="jrunjob server port.",
                        metavar="<int>")
    color_description(parser)
    show_help_on_empty_command()
    return parser


def client_parser():
    parser = argparse.ArgumentParser(
        description="send job status (file socket).",
        formatter_class=CustomHelpFormatter)
    parser.add_argument("-f", "--file", type=str, help="socket file.",
                        metavar="<file>")
    parser.add_argument("-H", "--host", type=str, help="runjob server hostname or ip.",
                        default="", metavar="<str>")
    parser.add_argument("-P", "--port", type=int, help="runjob server port.",
                        metavar="<int>")
    parser.add_argument("-n", "--name", type=str, help="job name.",
                        required=True, metavar="<str>")
    parser.add_argument("-s", "--status", type=str, help="job status.",
                        required=True, metavar="<str>")
    color_description(parser)
    show_help_on_empty_command()
    return parser


def get_config_args(parser):
    args = parser.parse_args()
    conf = load_config()
    if args.ini:
        conf.update_config(args.ini)
    conf.update_args(args)
    if args.config:
        print_config(conf)
        parser.exit()
    if args.jobfile.isatty():
        parser.print_help()
        parser.exit()
    return conf, conf.args
