#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
import signal
import argparse
import logging
import functools

from threading import Thread
from datetime import datetime
from collections import Counter
from subprocess import call, PIPE

from .utils import *
from .qsub import qsub
from .version import __version__


class ParseSingal(Thread):
    def __init__(self, clear=False, qjobs=None):
        super(ParseSingal, self).__init__()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.clear = clear
        self.qjobs = qjobs

    def run(self):
        time.sleep(1)

    def signal_handler(self, signum, frame):
        cleanAll(clear=self.clear, qjobs=self.qjobs)
        sys.exit(signum)


def LogExc(f):
    @functools.wraps(f)
    def wrapper(*largs, **kwargs):
        try:
            res = f(*largs, **kwargs)
            return res
        except Exception as e:
            logging.getLogger().exception(e)
            sys.exit(1)
    return wrapper


def parseArgs():
    parser = argparse.ArgumentParser(
        description="For manger submit your jobs in a job file.")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time, default: 1000", default=1000, metavar="<int>")
    parser.add_argument("-j", "--jobfile", type=str, required=True,
                        help="the input jobfile", metavar="<jobfile>")
    parser.add_argument('-i', '--injname', help="job names you need to run, default: all jobnames of you job file",
                        nargs="*", type=str, metavar="<str>")
    parser.add_argument('-s', '--start', help="job beginning with the number(0-base) you given, 0 by default",
                        type=int, default=0, metavar="<int>")
    parser.add_argument('-e', '--end', help="job ending with the number you given, last job by default",
                        type=int, metavar="<int>")
    parser.add_argument('-r', '--resub', help="rebsub you job when error, 0 or minus means do not re-submit, 3 times by default",
                        type=int, default=3, metavar="<int>")
    parser.add_argument('-ivs', '--resubivs', help="rebsub interval seconds, 2 by default",
                        type=int, default=2, metavar="<int>")
    parser.add_argument("-m", '--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost"], help="the mode to submit your jobs, 'sge' by default, if no sge installed, always localhost.")
    parser.add_argument("-nc", '--noclean', action="store_false", help="whether to clean all jobs or subprocess created by this programe when the main process exits, default: clean.",
                        default=True)
    parser.add_argument("--strict", action="store_true", default=False,
                        help="use strict to run. Means if any errors occur, clean all jobs and exit programe. off by default")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='log debug info', default=False)
    parser.add_argument("-l", "--log", type=str,
                        help='append log info to file, sys.stdout by default', metavar="<file>")
    parser.add_argument('-v', '--version',
                        action='version', version="%(prog)s v" + __version__)
    return parser.parse_args()


@LogExc
def main():
    args = parseArgs()
    logger = Mylog(logfile=args.log, level="debug" if args.debug else "info")
    qjobs = qsub(args.jobfile, args.num, args.injname,
                 args.start, args.end, mode=args.mode, usestrict=args.strict, clear=args.noclean)
    h = ParseSingal(clear=args.noclean, qjobs=qjobs)
    h.start()
    qjobs.run(times=args.resub, resubivs=args.resubivs)
    success = sumJobs(qjobs)
    if success:
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
