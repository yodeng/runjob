#!/usr/bin/env python2
# coding:utf-8

import os
import re
import sys

from subprocess import check_output

from .utils import RUNSTAT


class Jobfile(object):
    def __init__(self, jobfile, mode=None):
        try:
            p = check_output("command -v qstat", shell=True)
            self.has_sge = True
        except:
            self.has_sge = False
        self._path = os.path.abspath(jobfile)
        if not os.path.exists(self._path):
            raise IOError("No such file: %s" % self._path)
        self._pathdir = os.path.dirname(self._path)
        self.logdir = os.path.join(self._pathdir, "log")
        if self.has_sge:
            self.mode = "sge" if mode is None else mode
        else:
            self.mode = "localhost"
        if "local" in self.mode:
            self.mode = "localhost"

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
                                    if i == o:
                                        continue
                                    orders.setdefault(o, set()).add(i)
                        else:
                            self.throw("order names (%s) not in job, (%s)" % (
                                o, " ".join(line)))
        return orders

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
                    self.logdir = os.path.normpath(
                        os.path.join(self._pathdir, line.split()[-1]))
                    continue
                if line == "job_begin":
                    if len(job) and job[-1] == "job_end":
                        jobs.append(Job(self, job))
                        job = [line, ]
                    else:
                        job.append(line)
                elif line.startswith("order"):
                    continue
                else:
                    job.append(line)
            if len(job):
                jobs.append(Job(self, job))
        self.totaljobs = jobs
        self.alljobnames = [j.name for j in jobs]
        thisjobs = []
        if names is not None:
            for jn in jobs:
                name = jn.name
                for namereg in names:
                    if re.search(namereg, name):
                        thisjobs.append(jn)
                        break
            return thisjobs
        jobend = len(jobs) if end is None else end
        thisjobs = self.totaljobs[start:jobend]
        return thisjobs

    def throw(self, msg):
        raise OrderError(msg)


class Job(object):
    def __init__(self, jobfile, rules):
        self.rules = rules
        self.jf = jobfile
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
        if self.jf.has_sge:
            # if localhost defined, run localhost whether sge installed.
            if self.jf.mode in ["localhost", "local"]:
                self.host = "localhost"
            else:
                # if self.host has been defined other mode in job, sge default.
                if self.host not in [None, "sge", "localhost", "local"]:
                    self.host = None
        else:
            self.host = "localhost"  # if not sge installed, only run localhost
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

    def write(self, fo):
        fo.write("job_begin\n")
        rules = self.rules
        host = None
        for r in rules:
            r = r.strip()
            if r.startswith("host"):
                host = r.split()[-1]
        for k, attr in self.__dict__.items():
            if hasattr(attr, '__call__'):
                continue
            if k == "cmd":
                fo.write("    cmd_begin\n")
                cmdlines = ["        " +
                            i.strip() + "\n" for i in attr.split("&&")]
                fo.writelines(cmdlines)
                fo.write("    cmd_end\n")
            elif k == "rules":
                continue
            elif k == "host":
                if host is None:
                    continue
                else:
                    fo.write("    host " + host + "\n")
            else:
                try:
                    line = "    " + k + " " + attr + "\n"
                except TypeError:
                    continue
                fo.write(line)
        fo.write("job_end\n")

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


class SGEfile(object):
    def __init__(self, jobfile, mode=None, name=None, logdir=None, workdir=None):
        try:
            p = check_output("command -v qstat", shell=True)
            self.has_sge = True
        except:
            self.has_sge = False
        self._path = os.path.abspath(jobfile)
        if not os.path.exists(self._path):
            raise IOError("No such file: %s" % self._path)
        self._pathdir = os.path.dirname(self._path)
        self.mode = mode
        if self.mode is None:  # self.mode is self.local
            if self.has_sge:
                self.mode = "sge"
            else:
                self.mode = "localhost"
        else:
            if self.mode:
                self.mode = "localhost"
            else:
                self.mode = "sge"
        self.logdir = os.path.abspath(logdir)
        if not os.path.isdir(self.logdir):
            os.makedirs(self.logdir)
        self.workdir = workdir
        self.name = name

    def jobshells(self, start=0, end=None):
        jobs = []
        job = ""
        with open(self._path) as fi:
            for n, line in enumerate(fi):
                if n < start:
                    continue
                elif end is not None and n > end:
                    continue
                line = line.strip().strip("&")
                if line.startswith("#"):
                    continue
                jobs.append(SGEJob(self, n, line))
        return jobs


class SGEJob(object):
    def __init__(self, sgefile, linenum=None, cmd=None):
        self.sf = sgefile
        name = self.sf.name
        if name is None:
            name = os.path.basename(self.sf._path) + "_" + str(os.getpid())
            if name[0].isdigit():
                name = "job_" + name
        self.cpu = 0
        self.mem = 0
        if "//" in cmd:
            self.rawstring = cmd.split("//")[0].strip()
            try:
                args = cmd.split("//")[1].strip().split()
                if "-c" in args:
                    cpuidx = args.index("-c")
                elif "--cpu" in args:
                    cpuidx = args.index("--cpu")
                cpu = max(int(args[cpuidx+1]), 1)
                self.cpu = cpu
            except:
                pass
            try:
                args = cmd.split("//")[1].strip().split()
                if "-m" in args:
                    memidx = args.index("-m")
                elif "--memory" in args:
                    memidx = args.index("--memory")
                mem = max(int(args[memidx+1]), 1)
                self.mem = mem
            except:
                pass
        else:
            self.rawstring = cmd.strip()
        self.jobname = name + "_%d" % linenum
        self.name = self.jobname
        self.linenum = linenum
        self.logfile = os.path.join(self.sf.logdir, os.path.basename(
            self.sf._path) + "_line%d.log" % self.linenum)
        self.cmd = "echo [`date +'%F %T'`] 'RUNNING...' && " + \
            self.rawstring + RUNSTAT
        self.subtimes = 0
        self.status = None
        if self.sf.mode == "localhost":
            self.host = "localhost"
        else:
            self.host = "sge"
