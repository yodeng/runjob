#!/usr/bin/env python2
# coding:utf-8

import os
import time
import random

from subprocess import call, PIPE, Popen
from threading import Thread
from Queue import Queue
from datetime import datetime
from collections import defaultdict

from job import Jobfile

RUNSTAT = " && echo [\`date +'%F %T'\`] SUCCESS || echo [\`date +'%F %T'\`] ERROR"


class qsub(object):
    def __init__(self, jobfile, max_jobs=None, jobnames=None, start=1, end=None):
        self.pid = os.getpid()
        self.jfile = jobfile
        self.is_run = False

        jf = Jobfile(self.jfile)
        self.jobs = jf.jobs(jobnames, start, end)
        self.totaljobs = jf.totaljobs
        self.totaljobdict = {jf.name: jf for jf in self.totaljobs}

        jf.parseorder()
        self.thisorder = jf.thisorder  # real ordes
        self.orders = jf.orders()  # total orders in job file
        self.firstjobnames = jf.firstjob  # init job names
        self.orders_rev = {}
        for k, v in self.orders.items():
            for i in v:
                self.orders_rev.setdefault(i, set()).add(k)

        # order job name miss
        order_all = set(self.orders.keys() + self.orders_rev.keys())
        if order_all < jf.alljobnames:
            self.extrajob = jf.alljobnames - order_all
            self.throw("There are jobs not defined in orders")

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

        self.error = set()
        self.success = set()
        self.thisjobs = set(self.jobdict.keys())

        for jn in self.jobdict:
            lf = os.path.join(self.logdir, jn + ".log")
            if os.path.isfile(lf):
                if self.jobstatus(jn) != "success":
                    os.remove(lf)
                else:
                    self.thisjobs.remove(jn)
        self.waitjob = {k: self.jobdict[k] for k in self.thisjobs}  # this jobs
        self.has_success = set(self.jobdict.keys()) - self.thisjobs

    def not_qsub(self, jobname):
        qs = os.popen('qstat -xml | grep %s_%d | wc -l' %
                      (jobname, self.pid)).read().strip()
        if int(qs) == 0:
            return True
        return False

    def jobstatus(self, jobname):
        status = "wait"
        logfile = os.path.join(self.logdir, jobname + ".log")
        if os.path.isfile(logfile):
            status = "submit"
            try:
                sta = os.popen('tail -n 1 %s' % logfile).read().split()[-1]
            except IndexError:
                status = "run"
                # if self.not_qsub(jobname) and self.is_run:                                                           ## job exit, qsub error
                #    self.throw("Error in %s job, probably because of qsub interruption."%jobname)
                return status
            if sta == "SUCCESS":
                status = "success"
                self.success.add(jobname)
            elif sta == "ERROR":
                status = "error"
                self.error.add(jobname)
            elif sta == "Exiting.":
                status = "exit"
            else:
                if "RUNNING..." in os.popen("sed -n '3p' %s" % logfile).read():
                    status = "run"
                    # if self.not_qsub(jobname) and self.is_run:                                                       ## job exit, qsub error
                    #    self.throw("Error in %s job, probably because of qsub interruption."%jobname)
        return status

    def firstjob(self):
        return [self.totaljobdict[i] for i in self.firstjobnames]

    def qsubCheck(self, num, sec=1, ):
        qs = 0
        while True:
            time.sleep(sec)  # check per 1 seconds if job pools
            if not self.jobqueue.full():
                continue
            qs = os.popen('qstat -xml | grep _%d | wc -l' %
                          self.pid).read().strip()
            qs = int(qs)
            if qs < num:
                [self.jobqueue.get() for _ in range(num-qs)]
            else:
                continue

    def run(self, sec=2, times=-1, resubivs=2):

        self.is_run = True
        self.times = times
        self.subtimes = defaultdict(lambda: self.times)

        firstqsub = self.firstjob()
        if self.max_jobs < 100:
            p = Thread(target=self.qsubCheck, args=(self.max_jobs,))
            p.setDaemon(True)
            p.start()
        prepare_sub = set()
        for job in firstqsub:
            self.submit(job)
            if job.name in self.orders_rev:
                # prepare_sub.update(
                #    [i for i in self.orders_rev[job.name] if self.jobstatus(i) != "success"])
                prepare_sub.update(
                    [i for i in self.orders_rev[job.name] if i in self.thisjobs])
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
                        self.successjob[jn] = self.totaljobdict[jn]
                    elif js == "error":
                        if self.subtimes[jn] < 0:
                            if self.times >= 0:
                                print "%s job resubmit/rerun %d times, still error" % (
                                    jn, self.times+1)
                            self.errjob[jn] = self.jobdict[jn]
                            # self.throw("Error jobs return, %s"%os.path.join(self.logdir, jn + ".log"))   ## if error, exit program
                        else:
                            time.sleep(resubivs)  # sleep, re-submit
                            self.submit(self.totaljobdict[jn], resub=True)
                            self.subtimes[jn] -= 1
                            subK = False
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
        self.finalstat(resubivs)

    def submit(self, job, resub=False):
        logfile = os.path.join(self.logdir, job.name + ".log")

        if self.jobstatus(job.name) == "success":
            if job.name in self.waitjob:
                self.waitjob.pop(job.name)
            return

        if resub:
            logcmd = open(logfile, "a")
            logcmd.write("\n" + job.cmd+"\n")
        else:
            logcmd = open(logfile, "w")
            logcmd.write(job.cmd+"\n")
        logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
        logcmd.flush()

        if job.name in self.localjobs:
            qsubline = "echo [\`date +'%F %T'\`] RUNNING... && " + \
                job.cmd + RUNSTAT
            cmd = "echo " + qsubline[qsubline.index("RUNNING..."):]
            cmd = cmd.replace("\\", "")
            if resub:
                cmd = cmd.replace("RUNNING", "RUNNING \\(re-run\\)")
            Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)
        elif job.name in self.qsubjobs:
            job.cmd = job.cmd.replace('"', "\\\"")
            qsubline = "echo [\`date +'%F %T'\`] RUNNING... && " + \
                job.cmd + RUNSTAT
            if self.max_jobs < 100:
                self.jobqueue.put(job.name, block=True, timeout=1080000)
            cmd = 'qsub %s -N %s_%d -o %s -j y <<< "%s"' % (
                job.sched_options, job.name, self.pid, logfile, qsubline)
            if resub:
                cmd = cmd.replace("RUNNING", "RUNNING \\(re-submit\\)")
            call(cmd, shell=True, stdout=logcmd, stderr=logcmd)
        logcmd.close()
        if job.name in self.waitjob:
            self.waitjob.pop(job.name)

    def finalstat(self, resubivs):
        finaljobs = set(self.jobdict.keys()) - self.success - self.error
        while True:
            time.sleep(2)
            if len(finaljobs) == 0:
                break
            for jn in finaljobs.copy():
                js = self.jobstatus(jn)
                if js == "success":
                    finaljobs.remove(jn)
                elif js == "error":
                    if self.subtimes[jn] < 0:
                        if self.times >= 0:
                            print "%s job resubmit/rerun %d times, still error" % (
                                jn, self.times+1)
                        self.errjob[jn] = self.jobdict[jn]
                        finaljobs.remove(jn)
                    else:
                        time.sleep(resubivs)  # sleep, re-submit
                        self.submit(self.totaljobdict[jn], resub=True)
                        self.subtimes[jn] -= 1
                else:
                    continue

    def throw(self, msg):
        raise RuntimeError(msg)
