#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
import signal
import logging
import threading

from datetime import datetime
from collections import Counter
from subprocess import Popen, call, PIPE

from .job import Jobfile
from .dag import DAG
from .utils import *


class qsub(object):
    def __init__(self, jobfile, max_jobs=None, jobnames=None, start=0, end=None, mode=None, usestrict=False, clear=False):
        self.pid = os.getpid()
        self.jfile = jobfile
        self.is_run = False
        self.usestrict = usestrict
        self.clear = clear

        jf = Jobfile(self.jfile, mode=mode)
        self.has_sge = jf.has_sge
        self.jobs = jf.jobs(jobnames, start, end)
        self.jobnames = [j.name for j in self.jobs]

        self.totaljobdict = {jf.name: jf for jf in jf.totaljobs}

        self.orders = jf.orders()
        self.localprocess = {}

        # duplicate job names
        if len(jf.alljobnames) != len(set(jf.alljobnames)):
            names = [i for i, j in Counter(jf.alljobnames).items() if j > 1]
            self.throw("duplicate job name: %s" % " ".join(names))

        self.jobsgraph = DAG()
        for k, v in self.orders.items():
            self.jobsgraph.add_node_if_not_exists(k)
            for i in v:
                self.jobsgraph.add_node_if_not_exists(i)
                self.jobsgraph.add_edge(i, k)

        for jn in self.jobsgraph.all_nodes.copy():
            if jn not in self.jobnames:
                self.jobsgraph.delete_node(jn)

        self.logdir = jf.logdir
        if not os.path.isdir(self.logdir):
            os.makedirs(self.logdir)

        self.logger.info("Total jobs to submit: %s" %
                         " ".join([j.name for j in self.jobs]))
        self.logger.info("All logs can be found in %s directory", self.logdir)

        self.has_success = set()
        for job in self.jobs[:]:
            lf = os.path.join(self.logdir, job.name + ".log")
            job.subtimes = 0
            if job.status is not None and job.status in ["done", "success"]:
                self.jobsgraph.delete_node_if_exists(job.name)
                self.has_success.add(job.name)
                self.jobs.remove(job)
                continue
            if os.path.isfile(lf):
                js = self.jobstatus(job)
                if js != "success":
                    os.remove(lf)
                    job.status = "wait"
                else:
                    self.jobsgraph.delete_node_if_exists(job.name)
                    self.has_success.add(job.name)
                    self.jobs.remove(job)
            else:
                job.status = "wait"

        if self.jobsgraph.size() == 0:
            for jb in self.jobs:
                self.jobsgraph.add_node_if_not_exists(jb.name)

        self.max_jobs = len(self.jobs) if max_jobs is None else min(
            max_jobs, len(self.jobs))

        self.jobqueue = myQueue(maxsize=max(self.max_jobs, 1))

    def jobstatus(self, job):
        jobname = job.name
        status = job.status
        logfile = os.path.join(self.logdir, jobname + ".log")
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
                with os.popen("sed -n '3p' %s" % logfile) as fi:
                    if "RUNNING..." in fi.read():
                        status = "run"
        if status != job.status and self.is_run:
            self.logger.info("job %s status %s", jobname, status)
            job.status = status
        return status

    def jobcheck(self, sec=1):
        rate_limiter = RateLimiter(max_calls=3, period=sec)
        while True:
            with rate_limiter:
                for jb in self.jobqueue.queue:
                    with rate_limiter:
                        try:
                            js = self.jobstatus(jb)
                        except:
                            continue
                        if js == "success":
                            if jb.name in self.localprocess:
                                self.localprocess[jb.name].wait()
                            self.jobqueue.get(jb)
                            self.jobsgraph.delete_node_if_exists(jb.name)
                        elif js == "error":
                            if jb.name in self.localprocess:
                                self.localprocess[jb.name].wait()
                            self.jobqueue.get(jb)
                            if jb.subtimes >= self.times + 1:
                                if self.usestrict:
                                    self.throw("Error jobs return(submit %d times, error), exist!, %s" % (jb.subtimes, os.path.join(
                                        self.logdir, jb.name + ".log")))  # if error, exit program
                                self.jobsgraph.delete_node_if_exists(jb.name)
                            else:
                                self.submit(jb)
                        elif js == "exit":
                            if self.usestrict:
                                self.throw("Error when submit, %s" % jb.name)

    def run(self, sec=2, times=3, resubivs=2):
        self.is_run = True
        self.times = max(times, 0)
        self.resubivs = max(resubivs, 0)

        for jn in self.has_success:
            self.logger.info("job %s status already success", jn)

        p = threading.Thread(target=self.jobcheck)
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

    def submit(self, job):
        if not self.is_run or job.status in ["run", "submit", "resubmit", "success"]:
            return

        logfile = os.path.join(self.logdir, job.name + ".log")

        self.jobqueue.put(job, block=True, timeout=1080000)

        with open(logfile, "a") as logcmd:
            if job.subtimes == 0:
                logcmd.write(job.cmd+"\n")
                job.status = "submit"
            elif job.subtimes > 0:
                logcmd.write("\n" + job.cmd+"\n")
                job.status = "resubmit"

            self.logger.info("job %s status %s", job.name, job.status)
            logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
            logcmd.flush()

            qsubline = "echo [`date +'%F %T'`] 'RUNNING...' && " + \
                job.cmd + RUNSTAT

            if (job.host is not None and job.host == "localhost") or not self.has_sge:
                cmd = "echo 'Your job (\"%s\") has been submitted in localhost' && " % job.name + qsubline
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                p = Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)
                self.localprocess[job.name] = p
            else:
                cmd = 'echo "%s" | qsub %s -N %s_%d -o %s -j y' % (
                    qsubline, job.sched_options, job.name, self.pid, logfile)
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                call(cmd, shell=True, stdout=logcmd, stderr=logcmd)
            job.subtimes += 1
            self.logger.debug("%s job submit %s times", job.name, job.subtimes)

    def throw(self, msg):
        if threading.current_thread().__class__.__name__ == '_MainThread':
            raise QsubError(msg)
        else:
            self.logger.info(msg)
            cleanAll(self.clear, self)
            os._exit(signal.SIGTERM)

    def writestates(self, outstat):
        summary = {j.name: j.status for j in self.jobs}
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(summary.values()))) + "\n\n")
            sumout = {}
            for k, v in summary.items():
                sumout.setdefault(v, []).append(k)
            for k, v in sorted(sumout.items()):
                fo.write(
                    k + " : " + ", ".join(sorted(v, key=lambda x: (len(x), x))) + "\n")

    @property
    def logger(self):
        return logging.getLogger()
