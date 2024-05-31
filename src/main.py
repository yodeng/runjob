import os
import sys

from .utils import *
from .context import context
from .run import RunJob, RunFlow
from .parser import flow_parser, job_parser, init_parser

context._backend = BACKEND


def execute(parser):
    init_parser(parser)
    args = context.args
    if not globals().get(args.func):
        return
    for backend in context._backend:
        if getattr(args, backend, None):
            args.mode = backend
    if args.dag or args.dag_extend:
        args.quiet = True
    if args.jobfile is sys.stdin:
        jobfile = args.jobfile.readlines()
        args.jobfile.close()
        args.jobfile = jobfile
    else:
        args.jobfile.close()
        args.jobfile = args.jobfile.name
    if args.func.lower().endswith("job"):
        if args.logdir:
            args.logdir = join(args.workdir, args.logdir)
        elif isinstance(args.jobfile, list):
            args.logdir = join(args.workdir, "{}_log_dir".format(parser.prog))
        else:
            args.logdir = join(args.workdir, "{}_{}_log_dir".format(
                parser.prog, basename(args.jobfile)))
    job = globals()[args.func](config=context.conf)
    try:
        job.run()
    except (JobFailedError, RunJobError) as e:
        if args.quiet:
            raise e
        parser.exit(10)
    except Exception as e:
        raise e


def runjob():
    execute(job_parser())


def runflow():
    execute(flow_parser())


def runbatch():
    context._backend.append("batchcompute")
    execute(job_parser())
