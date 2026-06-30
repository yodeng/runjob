import io
import os
import sys
import argparse

from .utils import *
from .context import context
from ._version import __version__

try:
    from rich_argparse import RichHelpFormatter as HelpFormatter

    _HAS_RICH_ARGPARSE = True
except ImportError:
    from argparse import HelpFormatter

    _HAS_RICH_ARGPARSE = False


class CustomHelpFormatter(HelpFormatter):
    """HelpFormatter that auto-appends default values and shows required markers."""

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

                return f"{', '.join(parts)} {args_string}"

            return ', '.join(parts)

    def _get_default_metavar_for_optional(self, action):
        return action.dest.upper()

    def _get_default_metavar_for_positional(self, action):
        return action.dest


class ShowConfigAction(argparse.Action):

    def __init__(self,
                 option_strings,
                 dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS,
                 help=None):
        super(ShowConfigAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        self.__remove_action(parser)
        self._show_config(parser)
        context.init_arg(parser)
        context.conf.print_config()
        parser.exit()

    def _show_config(self, parser):
        pass

    def __remove_action(self, parser):
        for action in parser._actions:
            if isinstance(action, ShowConfigAction):
                break
        for opt in action.option_strings:
            if opt in sys.argv[1:]:
                sys.argv.remove(opt)


def show_help_on_empty_command():
    if len(sys.argv) == 1:
        sys.argv.append('--help')


def show_config(p):
    p.add_argument('--show-config',  action=ShowConfigAction,
                   help="show configurations and exit.")


def color_description(parser):
    """Apply red+underline style to the parser description.

    Uses Rich markup when rich_argparse is active (so RichHelpFormatter
    preserves the style across line wraps), and raw ANSI escapes otherwise.
    """
    if _HAS_RICH_ARGPARSE:
        parser.description = (
            f"[underline][red]{parser.description}[/red][/underline]"
        )
    else:
        # Apply ANSI styling per-line so explicit newlines don't drop color.
        parser.description = "\n".join(
            style(line, fore="red", mode="underline")
            for line in parser.description.split("\n")
        )


def rate_parser(p):
    rate_args = p.add_argument_group("rate arguments — submission rate limiting and retry controls")
    rate_args.add_argument('-r', '--retry', help="retry failed jobs N times (0 to disable)",
                           type=int, default=0, metavar="<int>")
    rate_args.add_argument('-R', '--retry-sec', help="wait N seconds before retrying a failed job",
                           type=int, default=2, metavar="<int>")
    rate_args.add_argument('--max-check', help="max job status checks per second",
                           type=float, default=DEFAULT_MAX_CHECK_PER_SEC, metavar="<float>")
    rate_args.add_argument('--max-submit', help="max job submissions per second",
                           type=float, default=DEFAULT_MAX_SUBMIT_PER_SEC, metavar="<float>")


def default_parser():
    p = argparse.ArgumentParser(add_help=False)
    base = p.add_argument_group("base arguments — core options for job submission and execution control")
    base.add_argument('-v', '--version',
                      action='version', version="v" + __version__)
    base.add_argument("-j", "--jobfile", type=argparse.FileType('r'), default=sys.stdin,
                      help="job file to read; reads from stdin if not specified " + REQUIRED, metavar="<jobfile>")
    base.add_argument("-n", "--num", type=int,
                      help="max concurrent jobs (default: all, max 1000)", metavar="<int>")
    base.add_argument("-s", "--start", type=int,
                      help="process starting from this line number (1-based)", metavar="<int>", default=1)
    base.add_argument("-e", "--end", type=int,
                      help="process up to this line number, inclusive (default: last line)", metavar="<int>")
    base.add_argument("-w", "--workdir", type=str, help="working directory",
                      default=abspath(os.getcwd()), metavar="<workdir>")
    base.add_argument('-d', '--debug', action='store_true',
                      help='enable debug logging', default=False)
    base.add_argument("-l", "--log", type=str,
                      help='write log output to file (default: stdout)', metavar="<file>")
    base.add_argument("-f", "--force", default=False, action="store_true",
                      help="force re-submit even if jobs already succeeded")
    base.add_argument('-M', '--mode', type=str, default="auto", choices=BACKEND,
                      help="job submission backend; auto-detected from command name if not set")
    base.add_argument('--config', metavar="<configfile>",
                      help="path to INI-style configuration file")
    base.add_argument("--dag", action="store_true", default=False,
                      help="print job DAG in DOT format and exit (no jobs run)")
    base.add_argument("--dag-extend", action="store_true", default=False,
                      help="print extended job DAG in DOT format and exit (no jobs run)")
    base.add_argument("--abort-on-error", action="store_true", default=False,
                      help="stop all jobs and exit on first error")
    base.add_argument("--quiet", action="store_true", default=False,
                      help="suppress all non-error output and logging")
    show_config(base)
    rate_parser(p)
    timeout_parser(p)
    backend_parser(p)
    return p


def timeout_parser(parser):
    time_args = parser.add_argument_group("time arguments — per-job timeout thresholds and retry limits")
    time_args.add_argument('--max-queue-time', help="max time in queue before timeout, e.g. 2h (default: no limit)",
                           type=str, metavar="<float/str>")
    time_args.add_argument('--max-run-time', help="max running time before timeout, e.g. 8h (default: no limit)",
                           type=str, metavar="<float/str>")
    time_args.add_argument('--max-wait-time', help="max total wait time before timeout, e.g. 4h (default: no limit)",
                           type=str, metavar="<float/str>")
    time_args.add_argument('--max-timeout-retry', help="retry timed-out jobs N times (0 to disable)",
                           type=int, default=0, metavar="<int>")


def backend_parser(parser):
    backend_args = parser.add_mutually_exclusive_group(required=False)
    for backend in BACKEND:
        backend_args.add_argument(f"--{backend}", default=False, action="store_true",
                                  help=f"submit jobs to {backend} backend")


def batch_parser(parser):
    batch = parser.add_argument_group("resource arguments — CPU, memory and node allocation for cluster backends (sge/slurm)")
    batch.add_argument("-q", "--queue", type=str, help="target queue or partition, space-separated (default: all)",
                       nargs="*", metavar="<queue>")
    batch.add_argument("-c", "--cpu", type=int,
                       help="CPUs per job", default=1, metavar="<int>")
    batch.add_argument("-m", "--memory", type=str,
                       help="memory per job (e.g. 4G, 8192M)", default="1G", metavar="<int/str>")
    batch.add_argument("--node", type=str, help="target node(s), space-separated (default: all)",
                       nargs="*", metavar="<node>")
    batch.add_argument("--round-node", action="store_true", help="round-robin nodes across jobs for load balancing",
                       default=False)


def job_parser():
    parser = argparse.ArgumentParser(
        description=(
            f"%(prog)s — submit shell commands as parallel batch jobs, with automatic "
            f"dependency resolution, retry, and status tracking.\n"
            f"Each non-empty line in the job file becomes an independent job. "
            f"Use 'wait' lines to declare dependencies between groups of jobs. "
            f"Supports {', '.join(BACKEND[1:])} backends."
        ),
        parents=[default_parser()],
        formatter_class=CustomHelpFormatter,
        allow_abbrev=False)
    parser.add_argument("-N", "--jobname", type=str,
                        help="base name prefix for generated job IDs (default: basename of jobfile)", metavar="<jobname>")
    parser.add_argument("-L", "--logdir", type=str,
                        help="directory for job log output (default: <workdir>/runjob_*_log_dir)", metavar="<logdir>")
    parser.add_argument("-g", "--groups", type=int, default=1,
                        help="group every N consecutive lines into a single job (default: 1)", metavar="<int>")
    parser.add_argument('--init', help="shell command to run before all jobs on localhost",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--callback', help="shell command to run after all jobs finish on localhost",
                        type=str,  metavar="<cmd>")
    batch_parser(parser)
    color_description(parser)
    parser.set_defaults(func="RunJob")
    return parser


def flow_parser():
    parser = argparse.ArgumentParser(
        description=(
            f"%(prog)s — run structured job flows with explicit dependencies and ordering.\n"
            f"Reads a job/flow file that defines named jobs and their execution order. "
            f"Supports pattern-based job filtering, multiple backend targets, and "
            f"DAG visualization. Backends: {', '.join(BACKEND[1:])}."
        ),
        parents=[default_parser()],
        formatter_class=CustomHelpFormatter,
        allow_abbrev=False)
    parser.add_argument('-i', '--match', help="run only job names matching these glob patterns (default: all)",
                        nargs="*", type=str, metavar="<str>")
    parser.add_argument("-L", "--logdir", type=str,
                        help="directory for job log output (default: <workdir>/logs)", metavar="<logdir>")
    batch_parser(parser)
    color_description(parser)
    parser.set_defaults(func="RunFlow")
    return parser


def shell_job_parser(arglist):
    parser = job_parser()
    return parser.parse_known_args(arglist)[0]


def server_parser():
    parser = argparse.ArgumentParser(
        description="%(prog)s — start a job status server that listens for status updates on a Unix or TCP socket.",
        formatter_class=CustomHelpFormatter)
    parser.add_argument("-f", "--file", type=str, help="Unix socket file path",
                        metavar="<file>")
    parser.add_argument("-H", "--host", type=str, help="server hostname or IP address",
                        metavar="<str>")
    parser.add_argument("-P", "--port", type=int, help="server port",
                        metavar="<int>")
    color_description(parser)
    show_help_on_empty_command()
    return parser


def client_parser():
    parser = argparse.ArgumentParser(
        description="%(prog)s — send a job status update to a running runjob-server via Unix or TCP socket.",
        formatter_class=CustomHelpFormatter)
    parser.add_argument("-f", "--file", type=str, help="Unix socket file path",
                        metavar="<file>")
    parser.add_argument("-H", "--host", type=str, help="server hostname or IP address",
                        default="", metavar="<str>")
    parser.add_argument("-P", "--port", type=int, help="server port",
                        metavar="<int>")
    parser.add_argument("-n", "--name", type=str, help="job name",
                        required=True, metavar="<str>")
    parser.add_argument("-s", "--status", type=str, help="job status",
                        required=True, metavar="<str>")
    color_description(parser)
    show_help_on_empty_command()
    return parser


def init_parser(parser):
    try:
        import argcomplete
        argcomplete.autocomplete(parser)
    except:
        pass
    args = parser.parse_args()
    context.init_arg(parser)
    if args.jobfile.isatty():
        parser.print_help()
        parser.exit()
    context.init_log(level=args.debug and "debug" or "info", logfile=args.log)
