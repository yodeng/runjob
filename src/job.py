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
        self._path = os.path.join(jobfile)
        if not os.path.exists(self._path):
            raise IOError("No such file: %s" % self._path)

        self.logdir = os.getcwd()

    def jobs(self):
        jobs = []
        job = []
        with open(self._path) as fi:
            for line in fi:
                if not line.strip() or line.startswith("#"):
                    continue
                line = line.strip()
                if line.startswith("log_dir"):
                    self.logdir = line.split()[-1]
                    continue
                if line == "job_begin":
                    if len(job):
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
        return jobs

    def orders(self):
        orders = {}
        with open(self._path) as fi:
            for line in fi:
                if not line.strip() or line.startswith("#"):
                    continue
                if line.startswith("order"):
                    line = line.split()
                    if line[2] == "after":
                        orders.setdefault(line[1], []).append(line[3])
                    elif line[2] == "before":
                        orders.setdefault(line[3], []).append(line[1])
        return orders


class Job(object):
    def __init__(self, rules):
        self.rules = rules
        self.name = None
        self.status = None
        self.sched_options = None
        self.cmd = None
        self.host = None
        self.checkrule()
        cmd = False
        for j in self.rules:
            j = j.strip()
            if not j or j.startswith("#"):
                continue
            if j in ["job_begin", "job_end", "cmd_end"]:
                continue
            elif j.startswith("name"):
                self.name = j.split()[-1]
            elif j.startswith("status"):
                self.status = j.split()[-1]
            elif j.startswith("sched_options"):
                self.sched_options = " ".join(j.split()[1:])
            elif j.startswith("host"):
                self.host = j.split()[-1]
            elif j == "cmd_begin":
                cmd = True
                continue
            else:
                if cmd:
                    self.cmd = j
                    cmd = False
                else:
                    # print rules
                    self.throw("Dup cmd in this job")

    def checkrule(self):
        rules = self.rules[:]
        if len(rules) <= 5:
            self.throw("rule list less then 5")
        if rules[0] != "job_begin" or rules[-1] != "job_end":
            self.throw("No start or end in you rule")
        rules = rules[1:-1]
        try:
            rules.remove("cmd_begin")
            rules.remove("cmd_end")
        except ValueError:
            self.throw("No start or end in you cmd")

    def throw(self, msg):
        raise RuleError(msg)


class RuleError(Exception):
    def __init__(self, ErrorInfo):
        super(RuleError, self).__init__(self)
        self.errorinfo = ErrorInfo

    def __str__(self):
        return self.errorinfo
    __repr__ = __str__
