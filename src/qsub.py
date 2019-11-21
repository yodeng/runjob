#!/usr/bin/env python2
# coding:utf-8

import os
import time
import logging

from subprocess import call, PIPE, Popen
from collections import defaultdict, Counter
from threading import Thread, Lock
from Queue import Queue
from datetime import datetime
from random import shuffle

from job import Jobfile

RUNSTAT = " && echo [\`date +'%F %T'\`] SUCCESS || echo [\`date +'%F %T'\`] ERROR"


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
            if v not in self._content:
                return "%s not in queue" % v
            else:
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
    def __init__(self, jobfile, max_jobs=None, jobnames=None, start=1, end=None, mode=None, usestrict=False):
        self.pid = os.getpid()
        self.jfile = jobfile
        self.is_run = False
        self.firstjobnames = set()
        self.state = {}
        self.usestrict = usestrict
        self.lock = Lock()

        jf = Jobfile(self.jfile, mode=mode)
        self.has_sge = jf.has_sge
        self.jobs = jf.jobs(jobnames, start, end)  # all jobs defined by args
        self.totaljobs = jf.totaljobs  # all jobs in job file
        self.totaljobdict = {jf.name: jf for jf in self.totaljobs}

        self.orders = jf.orders()  # total orders in job file
        self.orders_rev = {}   # total orders_rev in job file
        for k, v in self.orders.items():
            for i in v:
                self.orders_rev.setdefault(i, set()).add(k)

        # duplicate job names
        if len(jf.alljobnames) != len(set(jf.alljobnames)):
            names = [i for i, j in Counter(jf.alljobnames).items() if j > 1]
            self.throw("duplicate job name: %s" % " ".join(names))

        # order job name miss
        order_all = set(self.orders.keys() + self.orders_rev.keys())
        if order_all < jf.alljobnames:
            self.extrajob = jf.alljobnames - order_all
            self.throw("There are jobs not defined in orders")

        self.logdir = jf.logdir
        if not os.path.isdir(self.logdir):
            os.makedirs(self.logdir)

        self.error = set()  # args jobs error
        self.success = set()  # args jobs success,   self.error + self.success = len(self.jobs)
        self.thisjobnames = set([j.name for j in self.jobs])
        self.has_success = set()
        self.logger.info("Total jobs to submit: %s" %
                         " ".join(self.thisjobnames))

        for jn in self.thisjobnames.copy():
            lf = os.path.join(self.logdir, jn + ".log")
            job = self.totaljobdict[jn]
            if job.status is not None and job.status in ["done", "success"]:
                self.thisjobnames.remove(jn)
                self.has_success.add(jn)
                self.success.add(jn)
                continue
            if os.path.isfile(lf):  # if log file not exists, will run
                js = self.jobstatus(jn)
                if js != "success":
                    os.remove(lf)
                    if js == "error":
                        # self.error.remove(jn)
                        self.state.pop(jn)
                else:
                    self.thisjobnames.remove(jn)  # thisjobs - has_success
                    self.has_success.add(jn)
                    self.success.add(jn)
            else:
                self.state[jn] = "wait"
        # thisjobnames are real jobs
        # len(self.has_success) + len(self.thisjobnames) = len(self.jobs)
        self.max_jobs = len(
            self.thisjobnames) if max_jobs is None else min(max_jobs, len(self.thisjobnames))

        self.jobqueue = myQueue(maxsize=self.max_jobs)

    def not_qsub(self, jobname):
        qs = os.popen('qstat -xml | grep %s_%d | wc -l' %
                      (jobname, self.pid)).read().strip()
        if int(qs) == 0:
            return True
        return False

    def jobstatus(self, jobname):
        js = self.state.get(jobname, "")
        if js == "success":
            return js
        elif js == "error":
            return js
        status = "wait"  # wait to submit
        logfile = os.path.join(self.logdir, jobname + ".log")
        if os.path.isfile(logfile):
            status = "submit"  # wait to run
            try:
                sta = os.popen('tail -n 1 %s' % logfile).read().split()[-1]
            except IndexError:
                status = "run"
                if js in ["submit", "resubmit"]:
                    self.logger.info("job %s status run", jobname)
                # if self.not_qsub(jobname) and self.is_run:                                                           ## job exit, qsub error
                #    self.throw("Error in %s job, probably because of qsub interruption."%jobname)
                self.state[jobname] = status
                return status
            if sta == "SUCCESS":
                status = "success"
                # self.success.add(jobname)
            elif sta == "ERROR":
                status = "error"
                # self.error.add(jobname)
            elif sta == "Exiting.":
                status = "exit"
            else:
                if "RUNNING..." in os.popen("sed -n '3p' %s" % logfile).read():
                    status = "run"
                    if js in ["submit", "resubmit"]:
                        self.logger.info("job %s status run", jobname)
                    # if self.not_qsub(jobname) and self.is_run:                                                       ## job exit, qsub error
                    #    self.throw("Error in %s job, probably because of qsub interruption."%jobname)
        self.state[jobname] = status
        return status

    def firstjob(self):
        queryjob_tmp = self.thisjobnames.copy()
        for j in self.thisjobnames:
            if j in self.orders:
                for bj in self.orders[j]:
                    if bj in self.thisjobnames:
                        queryjob_tmp.remove(j)
                        break
        self.firstjobnames.update(queryjob_tmp)
        return [self.totaljobdict[i] for i in self.firstjobnames]

    def jobcheck(self, lock, sec=2):
        while True:
            time.sleep(sec/2)
            for jn in self.jobqueue.queue:
                time.sleep(sec/2)
                js = self.jobstatus(jn)
                if js == "success":
                    if jn not in self.success:
                        lock.acquire()
                        self.logger.info("job %s status %s", jn, js)
                        self.success.add(jn)
                        lock.release()
                    if jn in self.error:
                        lock.acquire()
                        self.error.remove(jn)
                        lock.release()
                    n = self.jobqueue.get(jn)
                elif js == "error":
                    if jn not in self.error:
                        lock.acquire()
                        self.logger.info("job %s status %s", jn, js)
                        self.error.add(jn)
                        lock.release()
                    n = self.jobqueue.get(jn)

    def run(self, sec=2, times=-1, resubivs=2):

        self.is_run = True
        self.times = times
        self.subtimes = defaultdict(lambda: self.times)

        for jn in self.has_success:
            self.logger.info("job %s status already success", jn)

        firstqsub = self.firstjob()
        firstjobnames = set([j.name for j in firstqsub])
        if self.max_jobs < len(self.thisjobnames):
            p = Thread(target=self.jobcheck, args=(self.lock,))
            p.setDaemon(True)
            p.start()
        prepare_sub = set()
        if len(firstqsub) <= self.max_jobs:
            for job in firstqsub:
                self.submit(job)
                if job.name in self.orders_rev:
                    # prepare_sub.update(
                    #    [i for i in self.orders_rev[job.name] if self.jobstatus(i) != "success"])
                    prepare_sub.update(
                        [i for i in self.orders_rev[job.name] if i in self.thisjobnames])
        else:
            prepare_sub = firstjobnames.copy()
            for j in prepare_sub:
                self.orders[j] = set([j])
        while len(self.thisjobnames) > 0:
            time.sleep(sec)
            tmp = list(prepare_sub.copy())
            shuffle(tmp)
            for k in tmp:
                time.sleep(0.1)
                subK = True
                tmp_ = list(self.orders[k])
                shuffle(tmp_)
                for jn in tmp_:
                    time.sleep(0.1)
                    js = self.jobstatus(jn)
                    if js == "success":
                        self.lock.acquire()
                        if jn not in self.success:
                            self.logger.info("job %s status %s", jn, js)
                        if jn in self.error:
                            self.error.remove(jn)
                        self.success.add(jn)
                        self.lock.release()
                        continue
                    elif js == "error":
                        if jn not in self.error:
                            self.lock.acquire()
                            self.logger.info("job %s status %s", jn, js)
                            self.error.add(jn)
                            self.lock.release()
                        if self.subtimes[jn] < 0:
                            if self.usestrict:
                                self.throw("Error jobs return(resubmit %d times, still error), exist!, %s" % (self.times+1, os.path.join(
                                    self.logdir, jn + ".log")))  # if error, exit program
                            continue
                        else:
                            self.lock.acquire()
                            self.error.remove(jn)
                            self.lock.release()
                            time.sleep(resubivs)  # sleep, re-submit
                            self.submit(self.totaljobdict[jn], resub=True)
                            self.subtimes[jn] -= 1
                            subK = False
                    elif js == "exit":
                        self.throw("Error when qsub")
                    else:
                        if jn == k:
                            continue
                        subK = False
                if subK:
                    self.submit(self.totaljobdict[k])
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

        # if submit, jobstatus must not be "success", so don't need to do this condition.
        # if self.jobstatus(job.name) == "success":
        #    if job.name in self.thisjobnames:
        #        self.thisjobnames.remove(job.name)
        #    return
        self.jobqueue.put(job.name, block=True, timeout=1080000)

        if resub:
            logcmd = open(logfile, "a")
            logcmd.write("\n" + job.cmd+"\n")
            self.state[job.name] = "resubmit"
            self.logger.info("job %s status resubmit", job.name)
        else:
            logcmd = open(logfile, "w")
            logcmd.write(job.cmd+"\n")
            self.state[job.name] = "submit"
            self.logger.info("job %s status submit", job.name)
        logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
        logcmd.flush()
        qsubline = "echo [\`date +'%F %T'\`] RUNNING... && " + \
            job.cmd + RUNSTAT
        if job.host is not None and job.host == "localhost":
            cmd = '''echo "Your job \\('%s'\\) has been submitted in localhost" && ''' % job.name + qsubline
            cmd = cmd.replace("\\", "")
            if resub:
                cmd = cmd.replace("RUNNING", "RUNNING \\(re-submit\\)")
            Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)
        else:
            job.cmd = job.cmd.replace('"', "\\\"")
            cmd = 'qsub %s -N %s_%d -o %s -j y <<< "%s"' % (
                job.sched_options, job.name, self.pid, logfile, qsubline)
            if resub:
                cmd = cmd.replace("RUNNING", "RUNNING \\(re-submit\\)")
            call(cmd, shell=True, stdout=logcmd, stderr=logcmd)
        logcmd.close()
        if job.name in self.thisjobnames:
            self.thisjobnames.remove(job.name)

    def finalstat(self, resubivs):
        finaljobs = set([j.name for j in self.jobs]) - self.has_success - \
            self.success - self.error
        while len(finaljobs) > 0:
            time.sleep(2)
            for jn in finaljobs.copy():
                js = self.jobstatus(jn)
                if js == "success":
                    finaljobs.remove(jn)
                    self.lock.acquire()
                    if jn not in self.success:
                        self.logger.info("job %s status %s", jn, js)
                    self.success.add(jn)
                    if jn in self.error:
                        self.error.remove(jn)
                    self.lock.release()
                elif js == "error":
                    if jn not in self.error:
                        self.lock.acquire()
                        self.logger.info("job %s status %s", jn, js)
                        self.error.add(jn)
                        self.lock.release()
                    if self.subtimes[jn] < 0:
                        finaljobs.remove(jn)
                        if self.usestrict:
                            self.throw("Error jobs return(resubmit %d times, still error), exist!, %s" % (self.times+1, os.path.join(
                                self.logdir, jn + ".log")))  # if error, exit program
                    else:
                        self.lock.acquire()
                        self.error.remove(jn)
                        self.lock.release()
                        time.sleep(resubivs)  # sleep, re-submit
                        self.submit(self.totaljobdict[jn], resub=True)
                        self.subtimes[jn] -= 1

    def throw(self, msg):
        raise RuntimeError(msg)

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
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(self.state.values()))) + "\n\n")
            sumary = {}
            for k, v in self.state.items():
                sumary.setdefault(v, []).append(k)
            for k, v in sorted(sumary.items(), key=lambda x: len(x[1])):
                fo.write(k + " : " + " ".join(v) + "\n")

    @property
    def logger(self):
        return logging.getLogger()
