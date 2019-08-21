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
    parser.add_argument("jobfile", type=str,
                        help="the input jobfile", metavar="<jobfile>")
    parser.add_argument('-v', '--version',
                        action='version', version=__version__)
    return parser.parse_args()


def main():
    args = parseArgs()
    h = ParseSingal()
    h.start()
    qjobs = qsub(args.jobfile, args.num)
    qjobs.run()
    if qjobs.error == 0:
        print("[%s] All tesks in file (%s) finished successfully." %
              (datetime.today().isoformat(), os.path.abspath(qjobs.jfile)))
    else:
        print "[%s] All tesks in file (%s) finished, But there are ERROR tesks." % (
            datetime.today().isoformat(qjobs.jfile),)
        print "Success jobs: %d" % qjobs.success
        print "Error jobs: %d" % qjobs.error


if __name__ == "__main__":
    main()
