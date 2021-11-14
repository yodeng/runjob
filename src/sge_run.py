#!/usr/bin/env python2
# coding:utf-8

import sys
import argparse
import time
import logging
import os

from job import SGEfile
from qsub import myQueue
from run import sumJobs, Mylog
from dag import DAG

from datetime import datetime
from threading import Thread
from subprocess import Popen, call
from collections import Counter


class RunSge(object):

    def __init__(self, sgefile, queue, cpu, mem, name, start, end, logdir, workdir, maxjob, block=False):
        self.sgefile = SGEfile(sgefile, mode=None, name=name,
                               logdir=logdir, workdir=workdir)
        self.jfile = self.sgefile._path
        self.jobs = self.sgefile.jobshells(start=start, end=end)
        self.totaljobdict = {j.jobname: j for j in self.jobs}
        self.queue = queue
        self.mem = mem
        self.cpu = cpu
        self.block = block
        self.maxjob = maxjob
        self.logdir = logdir
        self.is_run = False

        self.jobsgraph = DAG()
        pre_dep = []
        dep = []
        for jb in self.jobs[:]:
            if jb.rawstring == "wait" and len(dep):
                self.jobs.remove(jb)
                pre_dep = dep
                dep = []
            else:
                if jb.rawstring == "wait":  # dup "wait" line
                    self.jobs.remove(jb)
                    continue
                self.jobsgraph.add_node_if_not_exists(jb.jobname)
                dep.append(jb)
                for i in pre_dep:
                    self.jobsgraph.add_node_if_not_exists(i.jobname)
                    self.jobsgraph.add_edge(i.jobname, jb.jobname)

        self.logger.info("Total jobs to submit: %s" %
                         ", ".join([j.name for j in self.jobs]))
        self.logger.info("All logs can be found in %s directory", self.logdir)

        self.has_success = set()
        for job in self.jobs[:]:
            lf = job.logfile
            job.subtimes = 0
            if os.path.isfile(lf):
                js = self.jobstatus(job)
                if js != "success":
                    os.remove(lf)
                    job.status = "wait"
                else:
                    self.jobsgraph.delete_node_if_exists(job.jobname)
                    self.has_success.add(job.jobname)
                    self.jobs.remove(job)
            else:
                job.status = "wait"

        if self.maxjob is None:
            self.maxjob = len(self.jobs)

        self.jobqueue = myQueue(maxsize=max(self.maxjob, 1))

    def jobstatus(self, job):
        jobname = job.jobname
        status = job.status
        logfile = job.logfile
        if os.path.isfile(logfile):
            try:
                with os.popen('tail -n 1 %s' % logfile) as fi:
                    sta = fi.read().split()[-1]
            except IndexError:
                if status not in ["submit", "resubmit"]:
                    status = "run"
            if sta == "SUCCESS":
                status = "success"
            elif sta == "ERROR":
                status = "error"
            elif sta == "Exiting.":
                status = "exit"
            else:
                if "RUNNING..." in os.popen("sed -n '3p' %s" % logfile).read():
                    status = "run"
        if status != job.status and self.is_run:
            self.logger.info("job %s status %s", jobname, status)
            job.status = status
        return status

    def jobcheck(self, sec=2):
        while True:
            time.sleep(0.5)
            for jb in self.jobqueue.queue:
                time.sleep(sec/2)
                js = self.jobstatus(jb)
                if js == "success":
                    self.jobqueue.get(jb)
                    self.jobsgraph.delete_node_if_exists(jb.jobname)
                elif js == "error":
                    self.jobqueue.get(jb)
                    if jb.subtimes > self.times + 1:
                        self.jobsgraph.delete_node_if_exists(jb.jobname)
                    else:
                        self.submit(jb)
                elif js == "exit":
                    self.throw("Error when submit")

    def submit(self, job):
        if not self.is_run or job.status in ["run", "submit", "resubmit", "success"]:
            return

        logfile = job.logfile

        self.jobqueue.put(job, block=True, timeout=1080000)

        with open(logfile, "a") as logcmd:
            if job.subtimes == 0:
                logcmd.write(job.rawstring+"\n")
                job.status = "submit"
            elif job.subtimes <= self.times + 1:
                logcmd.write("\n" + job.rawstring+"\n")
                job.status = "resubmit"

            self.logger.info("job %s status %s", job.name, job.status)
            logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
            logcmd.flush()

            if job.host is not None and job.host == "localhost":
                cmd = 'echo Your job \("%s"\) has been submitted in localhost && ' % job.name + job.cmd
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING \(re-submit\)")
                    time.sleep(self.resubivs)
                Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)
            else:
                cmd = 'echo "%s" | qsub -q %s -wd %s -N %s -o %s -j y -l vf=%dg,p=%d' % (
                    job.cmd, " -q ".join(self.queue), self.sgefile.workdir, job.jobname, logfile, self.mem, self.cpu)
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING \(re-submit\)")
                    time.sleep(self.resubivs)
                call(cmd, shell=True, stdout=logcmd, stderr=logcmd)
            self.logger.debug("%s job submit %s times", job.name, job.subtimes)
            job.subtimes += 1

    def run(self, sec=2, times=3, resubivs=2):
        self.is_run = True
        self.times = max(0, times)   # 最大重投次数
        self.resubivs = max(resubivs, 0)

        for jn in self.has_success:
            self.logger.info("job %s status already success", jn)

        p = Thread(target=self.jobcheck)
        p.setDaemon(True)
        p.start()

        while True:
            subjobs = self.jobsgraph.ind_nodes()
            if len(subjobs) == 0:
                break
            for j in subjobs:
                jb = self.totaljobdict[j]
                if jb in self.jobqueue.queue:
                    continue
                self.submit(jb)
            time.sleep(sec)

    @property
    def logger(self):
        return logging.getLogger()

    def throw(self, msg):
        raise QsubError(msg)

    def writestates(self, outstat):
        summary = {j.name: j.status for j in self.jobs}
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(summary.values()))) + "\n\n")
            sumout = {}
            for k, v in summary.items():
                sumout.setdefault(v, []).append(k)
            for k, v in sorted(sumout.items(), key=lambda x: len(x[1])):
                fo.write(k + " : " + ", ".join(v) + "\n")


def parserArg():
    pid = os.getpid()
    parser = argparse.ArgumentParser(
        description="For multi-run your shell scripts localhost or qsub.")
    parser.add_argument("-q", "--queue", type=str, help="the queue your job running, default: all.q",
                        default=["all.q", ], nargs="*", metavar="<queue>")
    parser.add_argument("-m", "--memory", type=int,
                        help="the memory used per command (GB), default: 1", default=1, metavar="<int>")
    parser.add_argument("-c", "--cpu", type=int,
                        help="the cpu numbers you job used, default: 1", default=1, metavar="<int>")
    parser.add_argument("-wd", "--workdir", type=str, help="work dir, default: %s" %
                        os.path.abspath(os.getcwd()), default=os.path.abspath(os.getcwd()), metavar="<workdir>")
    parser.add_argument("-N", "--jobname", type=str,
                        help="job name", metavar="<jobname>")
    parser.add_argument("-o", "--logdir", type=str,
                        help='the output log dir, default: "runjob_*_log_dir"', metavar="<logdir>")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time. default: all in your job file", metavar="<int>")
    parser.add_argument("-s", "--startline", type=int,
                        help="which line number(0-base) be used for the first job tesk. default: 0", metavar="<int>", default=0)
    parser.add_argument("-e", "--endline", type=int,
                        help="which line number (include) be used for the last job tesk. default: all in your job file", metavar="<int>")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='log debug info', default=False)
    parser.add_argument("-l", "--log", type=str,
                        help='append log info to file, sys.stdout by default', metavar="<file>")
    parser.add_argument('-r', '--resub', help="rebsub you job when error, 0 or minus means do not re-submit, 3 times by default",
                        type=int, default=3, metavar="<int>")
    parser.add_argument('-ivs', '--resubivs', help="rebsub interval seconds, 2 by default",
                        type=int, default=2, metavar="<int>")
    # parser.add_argument("-b", "--block", action="store_true", default=False,
    # help="if passed, block when submitted you job, default: off")
    parser.add_argument("jobfile", type=str,
                        help="the input jobfile", metavar="<jobfile>")
    progargs = parser.parse_args()
    if progargs.logdir is None:
        progargs.logdir = os.path.join(os.path.abspath(os.path.dirname(
            progargs.jobfile)), "runjob_"+os.path.basename(progargs.jobfile) + "_log_dir")
    return progargs


def main():
    args = parserArg()
    logger = Mylog(logfile=args.log, level="debug" if args.debug else "info")
    runsge = RunSge(args.jobfile, args.queue, args.cpu, args.memory, args.jobname,
                    args.startline, args.endline, args.logdir, args.workdir, args.num)
    runsge.run(times=args.resub - 1, resubivs=args.resubivs)
    sumJobs(runsge)


if __name__ == "__main__":
    main()
