#!/usr/bin/env python2
# coding:utf-8

import os
import sys
from commands import getstatusoutput


class Jobfile(object):
    def __init__(self, jobfile):
        has_qstat = True if getstatusoutput(
            'command -v qstat')[0] == 0 else False
        if not has_qstat:
            print "No qsub command..."
            sys.exit(1)

        self._path = os.path.abspath(jobfile)
        if not os.path.exists(self._path):
            raise IOError("No such file: %s" % self._path)
        self._pathdir = os.path.dirname(self._path)
        self.logdir = os.path.join(self._pathdir, "log")
        self.alljobnames = None
        self.thisorder = {}
        self.firstjob = set()

    def jobs(self, names=None, start=1, end=None):  # real this jobs, not total jobs
        jobs = []
        job = []
        with open(self._path) as fi:
            for line in fi:
                if not line.strip() or line.strip().startswith("#"):
                    continue
                if "#" in line:
                    line = line[line.index("#"):]
                line = line.strip()
                if line.startswith("log_dir"):
                    self.logdir = os.path.join(self._pathdir, line.split()[-1])
                    continue
                if line == "job_begin":
                    if len(job) and job[-1] == "job_end":
                        jobs.append(Job(job))
                        job = [line, ]
                    else:
                        job.append(line)
                elif line.startswith("order"):
                    continue
                else:
                    job.append(line)
            if len(job):
                jobs.append(Job(job))
        self.totaljobs = jobs
        self.alljobnames = set([j.name for j in self.totaljobs])
        thisjobs = []
        if names is not None:
            for jn in jobs:
                if jn.name in names:
                    thisjobs.append(jn)
            self.thisjobnames = set([j.name for j in thisjobs])
            return thisjobs
        jobend = len(jobs) if end is None else end
        thisjobs = self.totaljobs[start-1:jobend]
        self.thisjobnames = set([j.name for j in thisjobs])
        return thisjobs

    def orders(self):
        orders = {}
        with open(self._path) as fi:
            for line in fi:
                if not line.strip() or line.startswith("#"):
                    continue
                if line.startswith("order"):
                    line = line.split()
                    if "after" in line:
                        idx = line.index("after")
                        o1 = line[1:idx]
                        o2 = line[idx+1:]
                    elif "before" in line:
                        idx = line.index("before")
                        o1 = line[idx+1:]
                        o2 = line[1:idx]
                    for o in o1:
                        if o in self.alljobnames:
                            for i in o2:
                                if i not in self.alljobnames:
                                    self.throw("order names (%s) not in job, (%s)" % (
                                        i, " ".join(line)))
                                else:
                                    orders.setdefault(o, set()).add(i)
                        else:
                            self.throw("order names (%s) not in job, (%s)" % (
                                o, " ".join(line)))
        return orders

    def parseorder(self):
        orders = self.orders()
        queryorder = {}
        first = set()
        queryjob = self.thisjobnames

        def pickorder(jn):
            jn2 = []
            for j in jn:
                if j in orders:
                    queryorder[j] = orders[j]
                    for v in orders[j]:
                        if v in orders:
                            queryorder[v] = orders[v]
                            jn2.extend(queryorder[v])
                        else:
                            first.add(v)
                else:
                    first.add(j)
            return jn2
        while True:
            if len(queryjob) == 0:
                break
            for i in queryjob:
                if i not in orders:
                    continue
                else:
                    break
            else:
                njn = pickorder(queryjob)
                break
            njn = pickorder(queryjob)
            queryjob = njn
        self.firstjob.update(first)
        self.thisorder = queryorder

    def throw(self, msg):
        raise OrderError(msg)


class Job(object):
    def __init__(self, rules):
        self.rules = rules
        self.name = None
        self.status = None
        self.sched_options = None
        self.cmd = []
        self.host = None
        self.checkrule()
        cmd = False
        for j in self.rules:
            j = j.strip()
            if not j or j.startswith("#"):
                continue
            if j in ["job_begin", "job_end"]:
                continue
            elif j.startswith("name"):
                name = j.split()[1:]
                if len(name) > 1:
                    self.name = self.name.replace(" ", "_")
                else:
                    self.name = name[-1]
            elif j.startswith("status"):
                self.status = j.split()[-1]
            elif j.startswith("sched_options"):
                self.sched_options = " ".join(j.split()[1:])
            elif j.startswith("host"):
                self.host = j.split()[-1]
            elif j == "cmd_begin":
                cmd = True
                continue
            elif j == "cmd_end":
                cmd = False
            elif j.startswith("cmd"):
                self.cmd.append(" ".join(j.split()[1:]))
            elif j.startswith("memory"):
                self.sched_options += " -l h_vmem=" + j.split()[-1].upper()
            elif j.startswith("time"):
                pass  # miss
                # self.sched_options += " -l h_rt=" + j.split()[-1].upper()   # hh:mm:ss
            else:
                if cmd:
                    self.cmd.append(j)
                else:
                    self.throw("cmd after cmd_end")
        for c in self.cmd:
            if c.startswith("exit"):
                self.throw(
                    "'exit' command not allow in the cmd string in %s job." % self.name)
        if len(self.cmd) > 1:
            self.cmd = " && ".join(self.cmd)
        elif len(self.cmd) == 1:
            self.cmd = self.cmd[0]
        else:
            self.throw("No cmd in %s job" % self.name)

    def checkrule(self):
        rules = self.rules[:]
        if len(rules) <= 4:
            self.throw("rules lack of elements")
        if rules[0] != "job_begin" or rules[-1] != "job_end":
            self.throw("No start or end in you rule")
        rules = rules[1:-1]
        if any([i.startswith("cmd") for i in rules]):
            return
        try:
            rules.remove("cmd_begin")
            rules.remove("cmd_end")
        except ValueError:
            self.throw("No start or end in %s job" % "\n".join(rules))

    def throw(self, msg):
        raise RuleError(msg)


class RuleError(Exception):
    def __init__(self, ErrorInfo):
        super(RuleError, self).__init__(self)
        self.errorinfo = ErrorInfo

    def __str__(self):
        return self.errorinfo
    __repr__ = __str__


class OrderError(Exception):
    def __init__(self, ErrorInfo):
        super(OrderError, self).__init__(self)
        self.errorinfo = ErrorInfo

    def __str__(self):
        return self.errorinfo
    __repr__ = __str__
