#!/usr/bin/env python2
# coding:utf-8

import os
import sys
import time
import signal
import argparse

from subprocess import call, PIPE
from threading import Thread
from datetime import datetime

from qsub import qsub
from version import __version__


class ParseSingal(Thread):
    def __init__(self):
        super(ParseSingal, self).__init__()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def run(self):
        time.sleep(1)

    def signal_handler(self, signum, frame):
        call('qdel "*_%d"' % os.getpid(), shell=True, stderr=PIPE, stdout=PIPE)
        sys.exit(signum)


def parseArgs():
    parser = argparse.ArgumentParser(
        description="For manger submit your jobs in a job file.")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time, default: 1000", default=1000, metavar="<int>")
    parser.add_argument("-j", "--jobfile", type=str, required=True,
                        help="the input jobfile", metavar="<jobfile>")
    parser.add_argument('-v', '--version',
                        action='version', version="%(prog)s v" + __version__)
    parser.add_argument('-i', '--injname', help="job names you need to run, default: all jobnames of you job file",
                        nargs="*", type=str, metavar="<str>")
    parser.add_argument('-s', '--start', help="job beginning with the number you given, 1 by default",
                        type=int, default=1, metavar="<int>")
    parser.add_argument('-e', '--end', help="job ending with the number you given, last job by default",
                        type=int, metavar="<int>")
    parser.add_argument('-r', '--resub', help="rebsub you job when error, 0 or minus means do not re-submit, 3 times by default",
                        type=int, default=3, metavar="<int>")
    parser.add_argument('-ivs', '--resubivs', help="rebsub interval seconds, 2 by default",
                        type=int, default=2, metavar="<int>")
    return parser.parse_args()


def main():
    args = parseArgs()
    h = ParseSingal()
    h.start()
    qjobs = qsub(args.jobfile, args.num, args.injname, args.start, args.end)
    qjobs.run(times=args.resub - 1, resubivs=args.resubivs)
    if len(qjobs.error) == 0:
        print("[%s] All tesks(%d, %d, %d) in file (%s) finished successfully." %
              (datetime.today().isoformat(), len(qjobs.success), len(qjobs.thisjobs), len({k: v for k, v in qjobs.subtimes.items() if v != args.resub - 1}), os.path.abspath(qjobs.jfile)))
    else:
        print "[%s] All tesks(%d, %d, %d) in file (%s) finished, But there are ERROR tesks." % (
            datetime.today().isoformat(), len(qjobs.success), len(qjobs.thisjobs), len({k: v for k, v in qjobs.subtimes.items() if v != args.resub - 1}), os.path.abspath(qjobs.jfile))
        print "Success jobs: %d" % len(qjobs.success)
        print "Error jobs: %d" % len(qjobs.error)


if __name__ == "__main__":
    main()
