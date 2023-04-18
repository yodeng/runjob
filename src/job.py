#!/usr/bin/env python
# coding:utf-8

import os
import re
import sys
import getpass
import tempfile

from .utils import *


@total_ordering
class Jobutils(object):

    def set_status(self, status=None):
        if not self.is_success:
            self.status = status

    def set_kill(self):
        if not self.is_end:
            self.set_status("kill")

    def remove_all_stat_files(self, remove_run=True):
        stats = [".success", ".error", ".submit"]
        self.remove_stat_file(*stats)
        if remove_run:
            self.remove_stat_file(".run")

    def remove_stat_file(self, *stat):
        for i in stat:
            fs = self.stat_file + i
            self.remove_file(fs)

    def remove_file(self, fs):
        try:
            os.remove(fs)
        except:
            pass

    def raw2cmd(self):
        self.stat_file = os.path.join(
            self.logdir, "."+os.path.basename(self.logfile))
        raw_cmd = self.rawstring
        if self.groups and self.groups > 1 and len(self.rawstring.split("\n")) > 1:
            if not os.path.isdir(self.logdir):
                os.makedirs(self.logdir)
            with open(self.stat_file + ".run", "w") as fo:
                fo.write(self.rawstring.strip() + "\n")
            raw_cmd = "/bin/bash -euxo pipefail " + self.stat_file + ".run"
        if self.host == "sge":
            self.cmd = "(echo [`date +'%F %T'`] 'RUNNING...' && rm -fr {0}.submit && touch {0}.run) && ".format(self.stat_file) + \
                "(%s)" % raw_cmd + \
                " && (echo [`date +'%F %T'`] SUCCESS && touch {0}.success && rm -fr {0}.run) || (echo [`date +'%F %T'`] ERROR && touch {0}.error && rm -fr {0}.run)".format(
                    self.stat_file)
        else:
            self.cmd = "(echo [`date +'%F %T'`] 'RUNNING...') && " + \
                "(%s)" % raw_cmd + RUNSTAT

    def qsub_cmd(self, mem=1, cpu=1):
        cmd = 'echo "{cmd}" | qsub -V -wd {workdir} -N {name} -o {logfile} -j y -l vf={mem}g,p={cpu} -S /bin/bash'
        cmd = cmd.format(
            cmd=self.cmd,
            workdir=self.workdir,
            name=self.jobpidname,
            logfile=self.logfile,
            mem=mem,
            cpu=cpu,
        )
        return cmd

    @property
    def is_fail(self):
        return self.status in ["kill", "error", "exit"]

    @property
    def is_end(self):
        return self.is_fail or self.is_success

    @property
    def is_success(self):
        return self.status == "success"

    @property
    def is_killed(self):
        return self.status == "kill"

    @property
    def is_wait(self):
        return self.status == "wait"

    @property
    def do_not_submit(self):
        return self.status in ["run", "submit", "resubmit", "success"]

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return self.name

    __str__ = __repr__

    def __hash__(self):
        return hash(self.name)

    @staticmethod
    def cmd2job(cmd="", name="", host=None, mem=1, cpu=1, queue=[]):
        '''
        @cmd <str or list>: required
        @name <str>: required
        @host <str>: default: None, {None,sge,local,localhost}
        @mem <int>: default: 1
        @cpu <int>: default: 1
        @queue <list>: default: all access queue
        '''
        job = textwrap.dedent('''
        job_begin
            name {name}
            host {host}
            sched_options{queue} -l vf={mem}g,p={cpu}
            cmd_begin
                {cmd} 
            cmd_end
        job_end
        ''').strip()
        if not cmd or not name:
            raise JobRuleError("cmd and name required")
        if len(queue):
            queue = " -q " + " -q ".join(queue)
        else:
            queue = ""
        if isinstance(cmd, (list, tuple)):
            cmd = ("\n" + " "*8).join(cmd)
        job = job.format(cmd=cmd.strip(), name=name, host=host,
                         mem=mem, cpu=cpu, queue=queue)
        return job+"\n"


class Jobfile(object):

    def __init__(self, jobfile, mode=None):
        self.has_sge = is_sge_submit()
        self._path = os.path.abspath(jobfile)
        if not os.path.exists(self._path):
            raise OSError("No such file: %s" % self._path)
        self._pathdir = os.path.dirname(self._path)
        self.logdir = os.path.join(self._pathdir, "log")
        self.workdir = os.getcwd()
        if self.has_sge:
            self.mode = mode or "sge"
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
        thisjobs = self.totaljobs[start-1:jobend]
        return thisjobs

    def throw(self, msg):
        raise JobOrderError(msg)


class Job(Jobutils):

    def __init__(self, jobfile, rules):
        self.rules = rules
        self.jf = jobfile
        self.logdir = self.jf.logdir
        self._path = self.jf._path
        self.name = None
        self.status = None
        self.sched_options = None
        self.cmd = []
        self.host = self.jf.mode
        self.checkrule()
        cmd = False
        self.groups = None
        self.queue = []
        for j in self.rules:
            j = j.strip()
            if not j or j.startswith("#"):
                continue
            if j in ["job_begin", "job_end"]:
                continue
            elif j.startswith("name"):
                name = re.split("\s", j, 1)[1].strip()
                name = re.sub("\s+", "_", name)
                if name.lower() == "none":
                    raise self.throw("None name in %s" % j)
                self.name = name
            elif j.startswith("status"):
                self.status = j.split()[-1]
            elif j.startswith("sched_options") or j.startswith("option"):
                self.sched_options = " ".join(j.split()[1:])
                self.cpu, self.mem = 1, 1
                args = self.sched_options.split()
                for n, i in enumerate(args[:]):
                    if i in ["-c", "--cpu"]:
                        self.cpu = max(int(args[n+1]), 1)
                    elif i in ["-m", "--memory"]:
                        self.mem = max(int(args[i+1]), 1)
                    elif i in ["-q", "--queue"]:
                        self.queue.append(args[n+1])
                    elif i == "-l":
                        res = args[n+1].split(",")
                        for r in res:
                            k = r.split("=")[0].strip()
                            v = r.split("=")[1].strip()
                            if k == "vf":
                                self.mem = int(re.sub("\D", "", v))
                            elif k in ["p", "np", "nprc"]:
                                self.cpu = int(v)
            elif j.startswith("host"):
                self.host = j.split()[-1]
            elif j == "cmd_begin":
                cmd = True
                continue
            elif j == "cmd_end":
                cmd = False
                continue
            elif cmd:
                self.cmd.append(j.strip())
            elif j.startswith("cmd"):
                self.cmd.append(" ".join(j.split()[1:]))
            elif j.startswith("memory"):
                self.sched_options += " -l h_vmem=" + j.split()[-1].upper()
            elif j.startswith("time"):
                pass  # miss
                # self.sched_options += " -l h_rt=" + j.split()[-1].upper()   # hh:mm:ss
        if self.jf.has_sge:
            if self.host not in ["sge", "localhost", "local"]:
                self.host = "sge"
        else:
            self.host = "localhost"
        if self.jf.mode in ["localhost", "local"]:
            self.host = "localhost"
        if len(self.cmd):
            self.groups = len(self.cmd)
            self.cmd = "\n".join(self.cmd)
        else:
            self.throw("No cmd in %s job" % self.name)
        self.rawstring = self.cmd.strip()
        self.cmd0 = self.cmd.strip()
        self.logfile = os.path.join(self.logdir, self.name + ".log")
        self.subtimes = 0
        self.raw2cmd()
        self.workdir = os.getcwd()
        self.jobname = self.name
        self.jobpidname = self.jobname + "_%d" % os.getpid()

    def checkrule(self):
        rules = self.rules
        if len(rules) <= 4:
            self.throw("rules lack of elements")
        if rules[0] != "job_begin" or rules[-1] != "job_end":
            self.throw("No start or end in you rule")
        if "cmd_begin" not in rules or "cmd_end" not in rules:
            self.throw("No start or end in %s job" % "\n".join(rules))

    def write(self, fo):
        cmd = self.cmd.split("\n")
        job = self.cmd2job(cmd=cmd, name=self.name, host=self.host,
                           mem=self.mem, cpu=self.cpu, queue=self.queue)
        fo.write(job)

    def throw(self, msg):
        raise JobRuleError(msg)


class ShellFile(object):

    def __init__(self, jobfile, mode=None, name=None, logdir=None, workdir=None):
        self.has_sge = is_sge_submit()
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
            raise OSError("No such file: %s" % self._path)
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
        if logdir is None:
            if self.temp:
                self.logdir = os.path.join(self.workdir, "runjob_log_dir")
            else:
                self.logdir = os.path.join(
                    self.workdir, "runjob_%s_log_dir" % os.path.basename(self._path))
        else:
            self.logdir = os.path.abspath(logdir)
        if not os.path.isdir(self.logdir):
            os.makedirs(self.logdir)
        if name is None:
            name = os.path.basename(self._path)
        if name[0].isdigit():
            name = "job_" + name
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


class ShellJob(Jobutils):

    def __init__(self, sgefile, linenum=None, cmd=None):
        self.sf = sgefile
        self.logdir = self.sf.logdir
        self._path = self.sf._path
        prefix = os.path.basename(self._path)
        name = self.sf.name
        self.cpu = 0
        self.mem = 0
        self.queue = None
        self.out_maping = None
        self.linenum = linenum + 1
        self.jobname = self.name = name + "_%05d" % self.linenum
        if self.sf.temp and name is not None:
            prefix = name
        self.logfile = os.path.join(
            self.logdir, prefix + "_%05d.log" % self.linenum)
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
        self.jobpidname = name + "_%d_%05d" % (os.getpid(), self.linenum)
        if self.host == "batchcompute":
            self.jobname = getpass.getuser() + "_" + \
                re.sub("[^a-zA-Z0-9_-]", "-", name + "_%05d" % self.linenum)
            self.name = self.jobname
            self.cmd = self.rawstring
            self.jobpidname = self.jobname.rsplit(
                "_", 1)[0] + "_%d_%05d" % (os.getpid(), self.linenum)

    def forceToLocal(self, jobname="", removelog=False):
        self.host = "localhost"
        self.name = self.jobname = jobname
        self.logfile = os.path.join(self.logdir, os.path.basename(
            self._path) + "_%s.log" % jobname)
        if removelog and os.path.isfile(self.logfile):
            os.remove(self.logfile)
        self.rawstring = self.cmd0.strip()
        self.raw2cmd()
