#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

import os
import sys

from .utils import *
from .context import context
from .run import RunJob, RunFlow
from .parser import flow_parser, job_parser, init_parser

# ── explicit job-class registry (avoids fragile globals() lookups) ──
_JOB_REGISTRY = {
    "RunJob":  RunJob,
    "RunFlow": RunFlow,
}


def _resolve_job_class(func_name):
    """Return the job class for *func_name*, or None."""
    return _JOB_REGISTRY.get(func_name)


def execute(parser):
    init_parser(parser)
    args = context.args
    job_cls = _resolve_job_class(args.func)
    if job_cls is None:
        return
    for backend in BACKEND:
        if getattr(args, backend, None):
            args.mode = backend
    if args.dag or args.dag_extend:
        args.quiet = True
    if args.jobfile is sys.stdin:
        jobfile = args.jobfile.readlines()
        args.jobfile.close()
        args.jobfile = jobfile
        context.conf._command_line_options["jobfile"] = True
    else:
        args.jobfile.close()
        args.jobfile = args.jobfile.name
    if args.func.lower().endswith("job"):
        if args.logdir:
            args.logdir = join(args.workdir, args.logdir)
        elif isinstance(args.jobfile, list):
            args.logdir = join(args.workdir, f"{parser.prog}_log_dir")
        else:
            args.logdir = join(args.workdir, f"{parser.prog}_{basename(args.jobfile)}_log_dir")
    job = job_cls(config=context.conf)
    try:
        job.run()
    except (JobFailedError, RunJobError) as e:
        if args.quiet:
            raise
        parser.exit(10)


def entry_exec():
    entry = basename(sys.argv[0])
    if entry in ("runflow", "runjob"):
        execute(flow_parser())
    elif entry in ("runsge", "runshell"):
        execute(job_parser())
