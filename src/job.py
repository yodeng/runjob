#!/usr/bin/env python
# coding:utf-8

import os
import re
import sys
import shlex
import getpass
import tempfile

from .utils import *
from .parser import shell_job_parser


@total_ordering
class Jobutils(object):

    def set_status(self, status=None):
        if not self.is_success and not self.is_killed:
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

    def raw2cmd(self, sleep_sec=0):
        self.stat_file = join(
            self.logdir, "."+basename(self.logfile))
        raw_cmd = self.rawstring
        if self.groups and self.groups > 1 and len(self.rawstring.split("\n")) > 1:
            raw_cmd = "/bin/bash -euxo pipefail -c " + \
                "'%s'" % "; ".join([i.strip()
                                   for i in self.rawstring.strip().split("\n") if i.strip()])
        if sleep_sec > 0:
            raw_cmd = "sleep %d && " % sleep_sec + raw_cmd
        if self.host == "sge":
            self.cmd = "(echo [`date +'%F %T'`] 'RUNNING...' && rm -fr {0}.submit && touch {0}.run) && " \
                       "({1}) && " \
                       "(echo [`date +'%F %T'`] SUCCESS && touch {0}.success && rm -fr {0}.run) || " \
                       "(echo [`date +'%F %T'`] ERROR && touch {0}.error && rm -fr {0}.run)".format(
                           self.stat_file, raw_cmd)
        else:
            self.cmd = "(echo [`date +'%F %T'`] 'RUNNING...') && " \
                       "({0}) && " \
                       "(echo [`date +'%F %T'`] SUCCESS) || " \
                       "(echo [`date +'%F %T'`] ERROR)".format(raw_cmd)

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

    def update_queue(self, queue=None):
        if queue:
            self.queue = set(self.queue)
            self.queue.update(queue)

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
        return "%s(%s)" % (self.__class__.__name__, self.name)

    __str__ = __repr__

    def __hash__(self):
        return hash(self.name)

    @staticmethod
    def cmd2job(cmd="", name="", host=None, mem=1, cpu=1, queue=None):
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
        if queue:
            queue = " -q " + " -q ".join(queue)
        else:
            queue = ""
        if isinstance(cmd, (list, tuple)):
            cmd = ("\n" + " "*8).join(cmd)
        job = job.format(cmd=cmd.strip(), name=name, host=host,
                         mem=mem, cpu=cpu, queue=queue)
        return job+"\n"

    def log_to_file(self, info=None):
        if info:
            with open(self.logfile, "a") as fo:
                s = "[{0}] {1}".format(
                    datetime.today().strftime("%F %X"), info)
                fo.write(s.strip()+"\n")
                fo.flush()


class Job(Jobutils):

    def __init__(self, config=None):
        self.rules = None
        self.jobfile = None
        self.logdir = None
        self._path = None
        self.name = None
        self.status = None
        self.sched_options = None
        self.cmd = None
        self.host = None
        self.groups = None
        self.cpu = 0
        self.mem = 0
        self.queue = set()
        self.subtimes = 0
        self.workdir = os.getcwd()
        self.out_maping = None
        self.cmd = ""
        self.cmd0 = ""
        self.run_time = 0   # runing time
        self.submit_time = 0  # submit time
        self.max_queue_sec = human2seconds(
            config and config.max_queue_time or sys.maxsize)
        self.max_run_sec = human2seconds(
            config and config.max_run_time or sys.maxsize)
        self.max_wait_sec = human2seconds(
            config and config.max_wait_time or sys.maxsize)
        self.max_timeout_retry = config and config.max_timeout_retry or 0
        self.timeout = False
        self.config = config

    def from_rules(self, jobfile=None, rules=None):
        self.rules = rules
        self.jobfile = jobfile
        self.logdir = self.jobfile.logdir
        self._path = self.jobfile._path
        self.host = self.jobfile.mode
        self.checkrule()
        _cmd = False
        cmds = []
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
                    raise JobRuleError("None name in %s" % j)
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
                        self.queue.add(args[n+1])
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
                _cmd = True
                continue
            elif j == "cmd_end":
                _cmd = False
                continue
            elif _cmd:
                cmds.append(j.strip())
            elif j.startswith("cmd"):
                cmds.append(" ".join(j.split()[1:]))
            elif j.startswith("memory"):
                self.sched_options += " -l h_vmem=" + j.split()[-1].upper()
            elif j.startswith("time"):
                pass  # miss
                # self.sched_options += " -l h_rt=" + j.split()[-1].upper()   # hh:mm:ss
        if self.host not in ["sge", "localhost", "local"]:
            self.host = "sge"
        if self.jobfile.mode in ["localhost", "local"] or not self.jobfile.has_sge:
            self.host = "localhost"
        if not len(cmds):
            raise JobRuleError("No cmd in %s job" % self.name)
        self.groups = len(cmds)
        self.cmd = "\n".join(cmds)
        self.rawstring = self.cmd.strip()
        self.cmd0 = self.cmd.strip()
        self.logfile = join(self.logdir, self.name + ".log")
        self.raw2cmd()
        self.jobname = self.name
        self.jobpidname = self.jobname + "_%d" % os.getpid()
        return self

    def from_cmd(self, sgefile, linenum=None, cmd=None):
        self.jobfile = sgefile
        self.logdir = self.jobfile.logdir
        self._path = self.jobfile._path
        name = self.jobfile.name
        self.linenum = linenum + 1
        self.jobname = self.name = name + "_%05d" % self.linenum
        prefix = self.jobfile.temp and name or basename(self._path)
        self.logfile = join(
            self.logdir, prefix + "_%05d.log" % self.linenum)
        self.host = self.jobfile.mode
        self.cmd0 = cmd
        self.workdir = abspath(self.jobfile.workdir)
        if re.search("\s+//", cmd) or re.search("//\s+", cmd):
            self.rawstring = cmd.rsplit("//", 1)[0].strip()
            try:
                argstring = shlex.split(cmd.rsplit("//", 1)[1].strip())
                args = shell_job_parser(argstring)
                for i in ['force', 'local', 'max_timeout_retry', 'workdir', 'jobname',
                          'groups', 'mode', 'queue', 'memory', 'cpu', 'out_maping']:
                    if getattr(args, i, False):
                        setattr(self, i, getattr(args, i))
                for i in ['max_queue_time', 'max_run_time', 'max_wait_time']:
                    k = i.replace("time", "sec")
                    t = min(human2seconds(
                        getattr(args, i, sys.maxsize)), getattr(self, k))
                    setattr(self, k, t)
                if getattr(args, "memory", None):
                    self.mem = getattr(args, "memory")
                if args.local:
                    args.mode = "local"
                if args.mode and args.mode in ["sge", "local", "localhost", "batchcompute"]:
                    self.host = args.mode
                if args.jobname and not args.jobname[0].isdigit():
                    self.jobname = self.name = args.jobname
                if self.host == "local":
                    self.host = "localhost"
                if self.jobfile.mode.startswith("local"):
                    self.host = self.jobfile.mode = "localhost"
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
        return self

    def to_local(self, jobname="", removelog=False):
        self.host = "localhost"
        self.name = self.jobname = jobname
        self.logfile = join(self.logdir, basename(
            self._path) + "_%s.log" % jobname)
        if removelog and isfile(self.logfile):
            os.remove(self.logfile)
        self.rawstring = self.cmd0.strip()
        self.raw2cmd()

    def checkrule(self):
        if self.rules:
            if len(self.rules) <= 4:
                raise JobRuleError("rules lack of elements")
            if self.rules[0] != "job_begin" or self.rules[-1] != "job_end":
                raise JobRuleError("No start or end in you rule")
            if "cmd_begin" not in self.rules or "cmd_end" not in self.rules:
                raise JobRuleError("No start or end in %s job" %
                                   "\n".join(self.rules))

    def get_job(self):
        cmd = self.cmd.split("\n")
        return self.cmd2job(cmd=cmd, name=self.name, host=self.host,
                            mem=self.mem, cpu=self.cpu, queue=self.queue)


class Jobfile(object):

    def __init__(self, jobfile, mode=None, workdir=os.getcwd(), config=None):
        self.config = config
        self.has_sge = is_sge_submit()
        self.workdir = abspath(workdir or os.getcwd())
        self.temp = None
        if isinstance(jobfile, (tuple, list)):
            self.temp = basename(tempfile.mktemp(prefix=__package__ + "_"))
            tmp_jobfile = join(self.workdir, self.temp)
            if not isdir(self.workdir):
                os.makedirs(self.workdir)
            with open(tmp_jobfile, "w") as fo:
                for line in jobfile:
                    fo.write(line.rstrip()+"\n")
            jobfile = tmp_jobfile
        self._path = abspath(jobfile)
        if not exists(self._path):
            raise IOError("No such file: %s" % self._path)
        self._pathdir = self.temp and self.workdir or dirname(self._path)
        self.logdir = join(self._pathdir, "log")
        self.mode = self.has_sge and (mode or "sge") or "localhost"
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
                                    raise JobOrderError("order names (%s) not in job, (%s)" % (
                                        i, " ".join(line)))
                                else:
                                    if i == o:
                                        continue
                                    orders.setdefault(o, set()).add(i)
                        else:
                            raise JobOrderError("order names (%s) not in job, (%s)" % (
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
                    self.logdir = normpath(
                        join(self._pathdir, line.split()[-1]))
                    continue
                if line == "job_begin":
                    if len(job) and job[-1] == "job_end":
                        jb = Job(self.config)
                        jobs.append(jb.from_rules(self, job))
                        job = [line, ]
                    else:
                        job.append(line)
                elif line.startswith("order"):
                    continue
                else:
                    job.append(line)
            if len(job):
                jb = Job(self.config)
                jobs.append(jb.from_rules(self, job))
        self.totaljobs = jobs
        self.alljobnames = [j.name for j in jobs]
        thisjobs = []
        if names:
            for jn in jobs:
                name = jn.name
                for namereg in names:
                    if re.search(namereg, name):
                        thisjobs.append(jn)
                        break
            return thisjobs
        jobend = not end and len(jobs) or end
        thisjobs = self.totaljobs[start-1:jobend]
        if self.temp and isfile(self._path):
            os.remove(self._path)
        return thisjobs


class Shellfile(Jobfile):

    def __init__(self, jobfile, mode=None, name=None, logdir=None, workdir=None, config=None):
        super(Shellfile, self).__init__(jobfile, mode, workdir, config=config)
        # "sge", "local", "localhost", "batchcompute"
        if self.mode not in ["sge", "local", "localhost", "batchcompute"]:
            self.mode = self.has_sge and "sge" or "localhost"
        if not logdir:
            if self.temp:
                self.logdir = join(self.workdir, __package__ + "_log_dir")
            else:
                self.logdir = join(
                    self.workdir, __package__ + "_%s_log_dir" % basename(self._path))
        else:
            self.logdir = abspath(logdir)
        if not isdir(self.logdir):
            os.makedirs(self.logdir)
        if not name:
            if self.temp:
                name = basename(self.logdir).split("_")[0]
            else:
                name = basename(self._path)
        if name[0].isdigit():
            name = "job_" + name
        self.name = name

    def jobs(self, start=0, end=None):
        jobs = []
        with open(self._path) as fi:
            for n, line in enumerate(fi):
                if n < start-1:
                    continue
                elif end and n > end-1:
                    continue
                line = line.strip().strip("&")
                if line.startswith("#") or not line.strip():
                    continue
                jb = Job(self.config)
                jobs.append(jb.from_cmd(self, n, line))
        if self.temp and isfile(self._path):
            os.remove(self._path)
        return jobs
