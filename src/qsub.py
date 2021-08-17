#!/usr/bin/env python2
# coding:utf-8

import os
import time
import logging

from subprocess import Popen
from collections import Counter
from threading import Thread
from Queue import Queue
from datetime import datetime

from job import Jobfile
from sge import RUNSTAT
from dag import DAG


class QsubError(Exception):
    pass


class myQueue(object):
    def __init__(self, maxsize=0):
        self._content = []
        self._queue = Queue(maxsize=maxsize)

    @property
    def length(self):
        return self._queue.qsize()

    def put(self, v, **kwargs):
        self._queue.put(v, **kwargs)
        self._content.append(v)

    def get(self, v=None):
        if v is None:
            o = self._content.pop(0)
            self._queue.get()
            return o
        else:
            if v in self._content:
                self._content.remove(v)
                self._queue.get()
                return v

    @property
    def queue(self):
        return self._content[:]

    def isEmpty(self):
        return self._queue.empty()

    def isFull(self):
        return self._queue.full()


class qsub(object):
    def __init__(self, jobfile, max_jobs=None, jobnames=None, start=0, end=None, mode=None, usestrict=False):
        self.pid = os.getpid()
        self.jfile = jobfile
        self.is_run = False
        self.firstjobnames = set()
        self.state = {}
        self.usestrict = usestrict
        self.times = 3

        jf = Jobfile(self.jfile, mode=mode)
        self.has_sge = jf.has_sge
        self.jobs = jf.jobs(jobnames, start, end)
        self.jobnames = [j.name for j in self.jobs]

        self.totaljobdict = {jf.name: jf for jf in jf.totaljobs}

        self.orders = jf.orders()

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

        for jn in self.jobsgraph.topological_sort():
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

        self.jobqueue = myQueue(maxsize=self.max_jobs)

    def not_qsub(self, jobname):
        qs = os.popen('qstat -xml | grep %s_%d | wc -l' %
                      (jobname, self.pid)).read().strip()
        if int(qs) == 0:
            return True
        return False

    def jobstatus(self, job):
        jobname = job.name
        status = job.status
        logfile = os.path.join(self.logdir, jobname + ".log")
        if os.path.isfile(logfile):
            try:
                sta = os.popen('tail -n 1 %s' % logfile).read().split()[-1]
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
            time.sleep(sec/2)
            for jb in self.jobqueue.queue:
                time.sleep(sec/2)
                js = self.jobstatus(jb)
                if js == "success":
                    self.jobqueue.get(jb)
                    self.jobsgraph.delete_node_if_exists(jb.name)
                elif js == "error":
                    self.jobqueue.get(jb)
                    if jb.subtimes >= self.times + 1:
                        self.jobsgraph.delete_node_if_exists(jb.name)
                        if self.usestrict:
                            self.throw("Error jobs return(resubmit %d times, still error), exist!, %s" % (self.times+1, os.path.join(
                                self.logdir, jb.name + ".log")))  # if error, exit program
                    else:
                        self.submit(jb)
                elif js == "exit":
                    self.throw("Error when submit")

    def subJobThread(self):
        n = 0
        while n == len(self.jobs):
            job = self.jobqueue.get()
            self.submit(job)
            n += 1

    def run(self, sec=2, times=3, resubivs=2):
        self.is_run = True
        self.times = times
        self.resubivs = resubivs

        for jn in self.has_success:
            self.logger.info("job %s status already success", jn)

        p = Thread(target=self.jobcheck)
        p.setDaemon(True)
        p.start()

        pre_subjobs = []
        while True:
            subjobs = self.jobsgraph.ind_nodes()
            if len(subjobs) == 0:
                break
            if subjobs == pre_subjobs:
                time.sleep(sec)
                continue
            for j in subjobs:
                jb = self.totaljobdict[j]
                if 0 <= jb.subtimes <= self.times + 1:
                    self.submit(jb)
                else:
                    self.jobsgraph.delete_node(j)
            time.sleep(sec)
            pre_subjobs = subjobs

    def submit(self, job):
        if not self.is_run:
            return

        logfile = os.path.join(self.logdir, job.name + ".log")

        self.jobqueue.put(job, block=True, timeout=1080000)

        if job.status in ["run", "submit", "resubmit", "success"]:
            return

        with open(logfile, "a") as logcmd:
            if job.subtimes == 0:
                logcmd.write(job.cmd+"\n")
                job.status = "submit"
                self.logger.info("job %s status %s", job.name, job.status)
            elif job.subtimes <= self.times + 1:
                logcmd.write("\n" + job.cmd+"\n")
                job.status = "resubmit"
                self.logger.info("job %s status %s", job.name, job.status)

            logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
            logcmd.flush()
            job.subtimes += 1
            qsubline = "echo [`date +'%F %T'`] RUNNING... && " + \
                job.cmd + RUNSTAT

            if job.host is not None and job.host == "localhost":
                cmd = 'echo Your job ("%s") has been submitted in localhost && ' % job.name + qsubline
                if job.subtimes > 1:
                    cmd = cmd.replace("RUNNING", "RUNNING \(re-submit\)")
                    time.sleep(self.resubivs)
            else:
                cmd = 'echo "%s" | qsub %s -N %s_%d -o %s -j y' % (
                    qsubline, job.sched_options, job.name, self.pid, logfile)
                if job.subtimes > 1:
                    cmd = cmd.replace("RUNNING", "RUNNING \(re-submit\)")
                    time.sleep(self.resubivs)
            Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)

    def throw(self, msg):
        raise QsubError(msg)

    def writejob(self, outjob):
        with open(outjob, "w") as fo:
            fo.write("log_dir " + self.logdir + "\n")
            for job in self.totaljobs:
                if job.name in self.state:
                    job.status = self.state[job.name]
                job.write(fo)
            for k, v in self.orders.items():
                for i in v:
                    fo.write("order %s after %s\n" % (k, i))

    def writestates(self, outstat):
        summary = {j.name: j.status for j in self.jobs}
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(summary.values()))) + "\n\n")
            sumout = {}
            for k, v in summary.items():
                sumout.setdefault(v, []).append(k)
            for k, v in sorted(sumout.items(), key=lambda x: len(x[1])):
                fo.write(k + " : " + ", ".join(v) + "\n")

    @property
    def logger(self):
        return logging.getLogger()
