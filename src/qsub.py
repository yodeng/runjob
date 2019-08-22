#!/usr/bin/env python2
# coding:utf-8

import os
import time
import random

from subprocess import call, PIPE
from threading import Thread
from Queue import Queue
from datetime import datetime

from job import Jobfile

RUNSTAT = " && echo [\`date +'%F %T'\`] SUCCESS || echo [\`date +'%F %T'\`] ERROR"


class qsub(object):
    def __init__(self, jobfile, max_jobs=None):
        self.pid = os.getpid()
        self.jfile = jobfile

        jf = Jobfile(self.jfile)
        self.orders = jf.orders()
        self.orders_rev = {}
        for k, v in self.orders.items():
            for i in v:
                self.orders_rev.setdefault(i, []).append(k)
        self.jobs = jf.jobs()
        self.jobdict = {jf.name: jf for jf in self.jobs}
        self.qsubjobs = {}
        self.localjobs = {}
        for j in self.jobs:
            if j.host is not None:
                if j.host == "localhost":
                    self.localjobs[j.name] = j
                else:
                    self.qsubjobs[j.name] = j
            else:
                self.qsubjobs[j.name] = j
        self.logdir = jf.logdir
        if not os.path.isdir(self.logdir):
            os.makedirs(self.logdir)
        self.max_jobs = len(self.qsubjobs) if max_jobs is None else max_jobs

        self.jobqueue = Queue(maxsize=self.max_jobs)
        self.successjob = {}
        self.errjob = {}
        self.waitjob = {k: v for k, v in self.jobdict.items()}  # all jobs

        for jn in self.jobdict:
            lf = os.path.join(self.logdir, jn + ".log")
            if os.path.isfile(lf):
                if self.jobstatus(jn) != "success":
                    os.remove(lf)

        self.error = 0
        self.success = 0

    def jobstatus(self, jobname):
        status = "wait"
        logfile = os.path.join(self.logdir, jobname + ".log")
        if os.path.isfile(logfile):
            status = "submit"
            try:
                sta = os.popen('tail -n 1 %s' % logfile).read().split()[-1]
            except IndexError:
                return "run"
            if sta == "SUCCESS":
                status = "success"
            elif sta == "Error":
                status = "error"
            elif sta == "Exiting.":
                status = "exit"
            else:
                if "RUNNING..." in os.popen("sed -n '3p' %s" % logfile).read():
                    status = "run"
        return status

    def firstjob(self):
        fj = []
        secondjobs = []
        for sj in self.orders:
            if self.jobstatus(sj) != "success":
                secondjobs.append(sj)
        for jn in self.jobs:
            if jn.name not in secondjobs:
                fj.append(jn)
        return fj

    def qsubCheck(self, num, sec=1, ):
        qs = 0
        while True:
            time.sleep(sec)  # check per 1 seconds
            if not self.jobqueue.full():
                continue
            qs = os.popen('qstat -xml | grep _%s | wc -l' %
                          self.pid).read().strip()
            qs = int(qs)
            if qs < num:
                [self.jobqueue.get() for _ in range(num-qs)]
            else:
                continue

    def run(self, sec=2):
        firstqsub = self.firstjob()
        if self.max_jobs < 100:
            p = Thread(target=self.qsubCheck, args=(self.max_jobs,))
            p.setDaemon(True)
            p.start()
        prepare_sub = set()
        print "Start Submit Jobs..."
        for job in firstqsub:
            self.submit(job)
            if job.name in self.orders_rev:
                prepare_sub.update(
                    [i for i in self.orders_rev[job.name] if self.jobstatus(i) != "success"])
        while True:
            time.sleep(sec)
            if len(self.waitjob) == 0:
                break
            tmp = list(prepare_sub)
            random.shuffle(tmp)
            for k in tmp:
                time.sleep(0.1)
                subK = True
                for jn in self.orders[k]:
                    time.sleep(0.1)
                    js = self.jobstatus(jn)
                    if js == "success":
                        self.successjob[jn] = self.jobdict[jn]
                    elif js == "error":
                        self.errjob[jn] = self.jobdict[jn]
                        # self.throw("Error jobs return, %s"%os.path.join(self.logdir, jn + ".log"))
                        pass
                    elif js == "exit":
                        self.throw("Error when qsub")
                    else:
                        subK = False
                if subK:
                    self.submit(self.jobdict[k])
                    if k in prepare_sub:
                        prepare_sub.remove(k)
                    if k in self.orders_rev:
                        prepare_sub.update(self.orders_rev[k])
                    for jn in self.orders[k]:
                        if jn in prepare_sub:
                            prepare_sub.remove(jn)
        self.finalstat()

    def submit(self, job):
        logfile = os.path.join(self.logdir, job.name + ".log")

        if self.jobstatus(job.name) == "success":
            if job.name in self.waitjob:
                self.waitjob.pop(job.name)
            return

        qsubline = "echo [\`date +'%F %T'\`] RUNNING... && " + \
            job.cmd + RUNSTAT

        logcmd = open(logfile, "w")
        logcmd.write(job.cmd+"\n")
        logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
        logcmd.flush()

        if job.name in self.localjobs:
            cmd = qsubline
            call(qsubline, shell=True, stdout=logcmd, stderr=logcmd)
        elif job.name in self.qsubjobs:
            if self.max_jobs < 100:
                self.jobqueue.put(job.name, block=True, timeout=1080000)
            cmd = 'qsub %s -N %s_%d -o %s -j y <<< "%s"' % (
                job.sched_options, job.name, self.pid, logfile, qsubline)
            call(cmd, shell=True, stdout=logcmd, stderr=logcmd)
        logcmd.close()
        if job.name in self.waitjob:
            self.waitjob.pop(job.name)

    def finalstat(self):
        alljobs = set(self.jobdict.keys())
        self.success = 0
        self.error = 0
        while True:
            time.sleep(2)
            if len(alljobs) == 0:
                break
            for jn in alljobs.copy():
                js = self.jobstatus(jn)
                if js == "success":
                    alljobs.remove(jn)
                    self.success += 1
                elif js == "error":
                    alljobs.remove(jn)
                    self.error += 1
                else:
                    continue

    def throw(self, msg):
        raise RuntimeError(msg)
