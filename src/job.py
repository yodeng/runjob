#!/usr/bin/env python
# coding:utf-8

import os
import re
import sys
import getpass
import tempfile

from subprocess import check_output

from .utils import *


class Jobfile(object):
    def __init__(self, jobfile, mode=None):
        self.has_sge = os.getenv("SGE_ROOT")
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
                self.cpu, self.mem = 1, 1
                args = self.sched_options.split()
                if "-l" in args:
                    resourceidx = args.index("-l")
                    res = args[resourceidx+1].split(",")
                    for r in res:
                        k = r.split("=")[0].strip()
                        v = r.split("=")[1].strip()
                        if k == "vf":
                            self.mem = int(re.sub("\D", "", v))
                        elif k in ["p", "np", "nprc"]:
                            self.cpu = int(v)
                if "-c" in args:
                    cpuidx = args.index("-c")
                    self.cpu = max(int(args[cpuidx+1]), 1)
                elif "--cpu" in args:
                    cpuidx = args.index("--cpu")
                    self.cpu = max(int(args[cpuidx+1]), 1)
                if "-m" in args:
                    memidx = args.index("-m")
                    self.mem = max(int(args[memidx+1]), 1)
                elif "--memory" in args:
                    memidx = args.index("--memory")
                    self.mem = max(int(args[memidx+1]), 1)
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

    def set_status(self, status=None):
        if self.status == "success":
            return
        self.status = status

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


class ShellFile(object):
    def __init__(self, jobfile, mode=None, name=None, logdir=None, workdir=None):
        self.has_sge = os.getenv("SGE_ROOT")
        self.workdir = workdir or os.getcwd()
        self.temp = None
        if isinstance(jobfile, (tuple, list)):
            self.temp = os.path.basename(tempfile.mktemp(prefix="runjob_"))
            tmp_jobfile = os.path.join(self.workdir, self.temp)
            if not os.path.isdir(self.workdir):
                os.makedirs(self.workdir)
            with open(tmp_jobfile, "w") as fo:
                for line in jobfile:
                    fo.write(line+"\n")
            jobfile = tmp_jobfile
        self._path = os.path.abspath(jobfile)
        if not os.path.exists(self._path):
            raise IOError("No such file: %s" % self._path)
        self._pathdir = os.path.dirname(self._path)
        self.mode = mode
        # "sge", "local", "localhost", "batchcompute"
        if self.mode not in ["sge", "local", "localhost", "batchcompute"]:
            if self.has_sge:
                self.mode = "sge"
            else:
                self.mode = "localhost"
        else:
            if self.mode in ["local", "localhost"]:
                self.mode = "localhost"
        if self.mode == "sge" and not self.has_sge:
            self.mode = "localhost"
        self.logdir = os.path.abspath(logdir)
        if not os.path.isdir(self.logdir):
            os.makedirs(self.logdir)
        self.name = name

    def jobshells(self, start=0, end=None):
        jobs = []
        with open(self._path) as fi:
            for n, line in enumerate(fi):
                if n < start-1:
                    continue
                elif end is not None and n > end-1:
                    continue
                line = line.strip().strip("&")
                if line.startswith("#") or not line.strip():
                    continue
                jobs.append(ShellJob(self, n, line))
        if self.temp and os.path.isfile(self._path):
            os.remove(self._path)
        return jobs


class ShellJob(object):
    def __init__(self, sgefile, linenum=None, cmd=None):
        self.sf = sgefile
        name = self.sf.name
        if name is None:
            name = os.path.basename(self.sf._path) + "_" + str(os.getpid())
            if name[0].isdigit():
                name = "job_" + name
        self.cpu = 0
        self.mem = 0
        self.queue = None
        self.out_maping = None
        self.linenum = linenum + 1
        self.jobname = name + "_%05d" % self.linenum
        self.name = self.jobname
        if self.sf.temp and self.sf.name is not None:
            self.logfile = os.path.join(
                self.sf.logdir, name+"_%d_line%05d.log" % (os.getpid(), self.linenum))
        else:
            self.logfile = os.path.join(self.sf.logdir, os.path.basename(
                self.sf._path) + "_line%05d.log" % self.linenum)
        self.subtimes = 0
        self.status = None
        self.host = self.sf.mode
        self.cmd0 = cmd
        self.groups = None
        self.workdir = self.sf.workdir
        if re.search("\s+//", cmd) or re.search("//\s+", cmd):
            self.rawstring = cmd.rsplit("//", 1)[0].strip()
            try:
                argstring = cmd.rsplit("//", 1)[1].strip().split()
                # argsflag = ["queue", "memory", "cpu", "jobname", "out_maping", "mode"]
                args = shellJobArgparser(argstring)
                for i in ["queue", "memory", "cpu", "out_maping", "workdir", "groups"]:
                    if getattr(args, i):
                        setattr(self, i, getattr(args, i))
                if getattr(args, "memory"):
                    self.mem = getattr(args, "memory")
                if args.local:
                    args.mode = "local"
                if args.mode and args.mode in ["sge", "local", "localhost", "batchcompute"]:
                    self.host = args.mode
                if args.jobname:
                    self.jobname = self.name = args.jobname
                if self.host == "local":
                    self.host = "localhost"
            except:
                pass
        else:
            self.rawstring = cmd.strip()
        self.raw2cmd()
        if self.host == "batchcompute":
            self.jobname = getpass.getuser() + "_" + \
                re.sub("[^a-zA-Z0-9_-]", "-", name + "_%05d" % self.linenum)
            self.name = self.jobname
            self.cmd = self.rawstring

    def raw2cmd(self):
        self.stat_file = os.path.join(
            self.sf.logdir, "."+os.path.basename(self.logfile))
        if self.host == "sge":
            self.cmd = "(echo [`date +'%F %T'`] 'RUNNING...' && rm -fr {0}.submit && touch {0}.run) && ".format(self.stat_file) + \
                self.rawstring + \
                " && (echo [`date +'%F %T'`] SUCCESS && touch {0}.success && rm -fr {0}.run) || (echo [`date +'%F %T'`] ERROR && touch {0}.error && rm -fr {0}.run)".format(
                    self.stat_file)
        else:
            self.cmd = "echo [`date +'%F %T'`] 'RUNNING...' && " + \
                self.rawstring + RUNSTAT

    def forceToLocal(self, jobname="", removelog=False):
        self.host = "localhost"
        self.name = self.jobname = jobname
        self.logfile = os.path.join(self.sf.logdir, os.path.basename(
            self.sf._path) + "_%s.log" % jobname)
        if removelog and os.path.isfile(self.logfile):
            os.remove(self.logfile)
        self.rawstring = self.cmd0.strip()
        self.raw2cmd()

    def set_status(self, status=None):
        if self.status == "success":
            return
        self.status = status
