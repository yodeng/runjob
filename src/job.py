#!/usr/bin/env python
# coding:utf-8

import os
import re
import sys
import shlex
import getpass
import tempfile

from glob import glob
from copy import copy
from itertools import product
from collections import OrderedDict

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

    def remove_all_stat_files(self):
        stats = [".success", ".error", ".submit", ".run"]
        self.remove_stat_file(*stats)

    def remove_stat_file(self, *stat):
        for i in stat:
            fs = self.stat_file + i
            self.remove_file(fs)

    def remove_file(self, fs):
        try:
            os.remove(fs)
        except:
            pass

    @property
    def stat_file(self):
        return join(self.logdir, "."+basename(self.logfile))

    def raw2cmd(self, sleep_sec=0):
        raw_cmd = self.raw_cmd
        if self.groups and self.groups > 1 and len(self.raw_cmd.split("\n")) > 1:
            raw_cmd = "/bin/bash -euxo pipefail -c " + \
                "'%s'" % "; ".join([i.strip()
                                   for i in self.raw_cmd.strip().split("\n") if i.strip()])
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
            raise JobError("cmd and name required")
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
        self.run_time = now()   # runing time
        self.submit_time = now()  # submit time
        self.max_queue_sec = human2seconds(
            config and config.max_queue_time or sys.maxsize)
        self.max_run_sec = human2seconds(
            config and config.max_run_time or sys.maxsize)
        self.max_wait_sec = human2seconds(
            config and config.max_wait_time or sys.maxsize)
        self.max_timeout_retry = config and config.max_timeout_retry or 0
        self.timeout = False
        self.config = config
        self.depends = set()
        self.extend = []

    def from_rules(self, jobfile=None, rules=None):
        self.rules = rules
        self.jobfile = jobfile
        self.logdir = self.jobfile.logdir
        self._path = self.jobfile._path
        self.host = self.jobfile.mode
        self.checkrule()
        cmds = self._parse_jobs(self.rules[:], no_begin=False)
        for n, c in enumerate(cmds[::-1]):
            self._get_cmd(c)
            cmds[len(cmds)-1-n] = self.raw_cmd
        self.groups = len(cmds)
        self.cmd = "\n".join(cmds)
        self.raw_cmd = self.cmd.strip()
        self.cmd0 = self.cmd.strip()
        self.logfile = join(self.logdir, self.name + ".log")
        self.raw2cmd()
        self.jobname = self.name
        self.jobpidname = self.jobname + "_%d" % os.getpid()
        return self

    def from_cmd(self, sgefile, linenum=None, cmd=None):
        self.rules = cmd.strip()
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
        self._get_cmd(cmd)
        self.raw2cmd()
        self.jobpidname = name + "_%d_%05d" % (os.getpid(), self.linenum)
        if self.host == "batchcompute":
            self.jobname = getpass.getuser() + "_" + \
                re.sub("[^a-zA-Z0-9_-]", "-", name + "_%05d" % self.linenum)
            self.name = self.jobname
            self.cmd = self.raw_cmd
            self.jobpidname = self.jobname.rsplit(
                "_", 1)[0] + "_%d_%05d" % (os.getpid(), self.linenum)
        return self

    def from_task(self, jobfile, tasks=None):
        self.rules = tasks
        self.jobfile = jobfile
        self.logdir = self.jobfile.logdir
        self._path = self.jobfile._path
        self.host = self.jobfile.mode
        cmds = self._parse_jobs(tasks[1:], no_begin=True)
        if not hasattr(self, "name") or not self.name or self.name.startswith("$"):
            name = tasks[0].strip(":").strip()
            if name in self.jobfile.totaljobs:
                raise KeyError(
                    "duplicate job name '{}': {}".format(name, tasks))
            elif re.search("\s", name):
                raise KeyError(
                    "no space allowed in jobname: '{}'".format(name))
            self.name = name
        for n, c in enumerate(cmds[::-1]):
            self._get_cmd(c)
            cmds[len(cmds)-1-n] = self.raw_cmd
        self.groups = len(cmds)
        self.cmd = "\n".join(cmds)
        self.raw_cmd = self.cmd.strip()
        self.cmd0 = self.cmd.strip()
        self.logfile = join(self.logdir, self.name + ".log")
        self.raw2cmd()
        self.jobname = self.name
        self.jobpidname = self.jobname + "_%d" % os.getpid()
        return self

    def _parse_jobs(self, lines=None, no_begin=False):
        cmds = []
        for j in lines:
            j = j.strip()
            if not j or j.startswith("#"):
                continue
            value = re.split("[\s:]", j, 1)[-1].strip(":").strip()
            if j in ["job_begin", "job_end"]:
                continue
            elif re.match("names?[\s:]", j):
                name = value
                name = re.sub("\s+", "_", name)
                if name.lower() == "none":
                    raise JobError("None name in %s" % j)
                self.name = name
            elif re.match("status?[\s:]", j):
                self.status = value
            elif re.match("sched_options?[\s:]", j) or \
                    re.match("options?[\s:]", j) or \
                    re.match("args?[\s:]", j) or \
                    re.match("qsub_args?[\s:]", j):
                self.sched_options = value
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
            elif re.match("depends?[\s:]", j):
                self.depends = set(self.jobfile._get_value_list(value))
            elif re.match("host[\s:]", j):
                self.host = value
            elif re.match("force[\s:]", j):
                force = value
                self.force = float(force) if force.isdigit() else force
            elif j == "cmd_begin":
                no_begin = True
                continue
            elif j == "cmd_end":
                no_begin = False
                continue
            elif re.match("cmd?[\s:]", j):
                cmds.append(value)
            elif re.match("extends?[\s:]", j):
                self.extend = self.jobfile._get_value_list(value)
            elif re.match("memory[\s:]", j) or re.match("mem[\s:]", j):
                self.mem = int(re.sub("\D", "", value))
            elif re.match("cpu[\s:]", j):
                self.cpu = int(value)
            elif re.match("queues?[\s:]", j):
                self.queue = self.jobfile._get_value_list(value)
            elif re.match("max[_-]queue[_-]time[\s:]", j):
                self.max_queue_sec = human2seconds(value)
            elif re.match("max[_-]run[_-]time[\s:]", j):
                self.max_run_sec = human2seconds(value)
            elif re.match("max[_-]wait[_-]time[\s:]", j):
                self.max_wait_sec = human2seconds(value)
            elif re.match("max[_-]timeout[_-]retry[\s:]", j):
                self.max_timeout_retry = int(value)
            elif re.match("time[\s:]", j):
                self.max_run_sec = human2seconds(value)
            elif no_begin and not ":" in j:
                cmds.append(j)
            else:
                raise JobError(
                    "unrecognized line error: {} {}".format(j, lines))
        cmds = list(filter(None, cmds))
        if not len(cmds):
            raise JobError("No cmd in %s job" % self.name)
        if self.host not in ["sge", "localhost", "local"]:
            self.host = "sge"
        if self.jobfile.mode in ["localhost", "local"] or not self.jobfile.has_sge:
            self.host = "localhost"
        return cmds

    def _get_cmd(self, cmd=None):
        if re.search("\s+//", cmd) or re.search("//\s+", cmd):
            self.raw_cmd = cmd.rsplit("//", 1)[0].strip()
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
            self.raw_cmd = cmd.strip()

    def to_local(self, jobname="", removelog=False):
        self.host = "localhost"
        self.name = self.jobname = jobname
        self.logfile = join(self.logdir, basename(
            self._path) + "_%s.log" % jobname)
        if removelog and isfile(self.logfile):
            os.remove(self.logfile)
        self.raw_cmd = self.cmd0.strip()
        self.raw2cmd()

    def checkrule(self):
        if self.rules:
            if len(self.rules) <= 4:
                raise JobError("rules lack of elements")
            if self.rules[0] != "job_begin" or self.rules[-1] != "job_end":
                raise JobError("No start or end in you rule")
            if "cmd_begin" not in self.rules or "cmd_end" not in self.rules:
                raise JobError("No start or end in %s job" %
                               "\n".join(self.rules))

    def get_job(self):
        cmd = self.cmd.split("\n")
        return self.cmd2job(cmd=cmd, name=self.name, host=self.host,
                            mem=self.mem, cpu=self.cpu, queue=self.queue)

    def copy(self):
        return copy(self)


class Jobfile(object):

    def __init__(self, jobfile, mode=None, workdir=os.getcwd(), config=None):
        self.config = config
        self.has_sge = is_sge_submit()
        self.workdir = abspath(workdir or os.getcwd())
        self.temp = None
        if isinstance(jobfile, (tuple, list)):
            self.temp = basename(tempfile.mktemp(prefix=__package__ + "_"))
            tmp_jobfile = join(self.workdir, self.temp)
            mkdir(self.workdir)
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
        self.totaljobs = OrderedDict()
        self.jobs = []
        self.orders = {}
        self.envs = {}
        self.job_set = {}

    def _read_orders(self):
        with open(self._path) as fi:
            for line in fi:
                if not line.strip() or line.startswith("#"):
                    continue
                line = line.split("#")[0].rstrip()
                if re.match("logs?_?(dir)?[\s:]", line) or re.match("includes?[\s:]", line):
                    continue
                elif line.startswith("order"):
                    line = line.split()
                    if "after" in line:
                        idx = line.index("after")
                        o1 = line[1:idx]
                        o2 = line[idx+1:]
                    elif "before" in line:
                        idx = line.index("before")
                        o1 = line[idx+1:]
                        o2 = line[1:idx]
                elif "->" in line:
                    o1, o2 = line.split("->")
                    o1 = self._get_value_list(o1)
                    o2 = self._get_value_list(o2)
                elif ":" in line and not line.strip().endswith(":") and not re.match("\s", line):
                    o1, o2 = line.split(":")
                    o1 = self._get_value_list(o1)
                    o2 = self._get_value_list(o2)
                elif "<-" in line:
                    o2, o1 = line.split("<-")
                    o1 = self._get_value_list(o1)
                    o2 = self._get_value_list(o2)
                elif not re.match("\s", line):
                    deps = re.search("(depends?([_\s])?\s*(on)?)", line)
                    if deps:
                        o1, o2 = line.split(deps.group())
                        o1 = self._get_value_list(o1)
                        o2 = self._get_value_list(o2)
                    else:
                        continue
                else:
                    continue
                for o in o1:
                    for i in o2:
                        if i == o:
                            continue
                        self.totaljobs[o].depends.add(i)

    def parse_orders(self):
        self._read_orders()
        self.expand_orders()
        self.check_orders_name()

    def parse_jobs(self, names=None, start=1, end=None):
        job = []
        _env = False
        with open(self._path) as fi:
            for _line in fi:
                if not _line.strip() or _line.strip().startswith("#"):
                    continue
                _line = _line.split("#")[0]
                line = _line.strip()
                if re.match("logs?_?(dir)?[\s:]", line):
                    self.logdir = normpath(
                        join(self._pathdir, line.split()[-1]))
                    continue
                elif re.match("includes?[\s:]", line):
                    value = re.split("[\s:]", line, 1)[-1].strip(":").strip()
                    includes = self._get_value_list(value)
                    for rule_path in includes:
                        rulefile = partial(join, dirname(self._path))
                        rulefiles = glob(rulefile(rule_path)) or glob(
                            rulefile("rules", rule_path))
                        for rf in rulefiles:
                            jfile = self.__class__(
                                rf, mode=self.mode, workdir=self.workdir, config=self.config)
                            jfile.logdir = self.logdir
                            jfile.parse_jobs()
                            jfile._read_orders()
                            self.envs.update(jfile.envs)
                            self.jobs.extend(jfile.jobs)
                            self.totaljobs.update(jfile.totaljobs)
                    continue
                elif re.match("envs?[\s:]", line) or re.match("var?[\s:]", line):
                    _env = True
                elif line == "job_begin":
                    if len(job) and job[-1] == "job_end":
                        self.add_job(job, method="from_rules")
                        job = []
                elif line.endswith(":"):
                    if len(job):
                        if not _env:
                            self.add_job(job, method="from_task")
                        else:
                            _env = False
                            self.add_envs(job)
                        job = []
                elif line.startswith("order") or "->" in line or \
                        "<-" in line or \
                        not re.match("\s", _line) and \
                        (":" in line or re.search("(depends?([_\s])?\s*(on)?)", line)):
                    continue
                job.append(line)
            if len(job):
                if job[0].endswith(":"):
                    self.add_job(job, method="from_task")
                else:
                    self.add_job(job, method="from_rules")
        if names:
            for name, jn in self.totaljobs.items():
                for namereg in names:
                    if re.search(namereg, name):
                        self.jobs.append(jn)
                        break
            return
        jobend = end or len(self.totaljobs)
        self.jobs = list(self.totaljobs.values())[start-1:jobend]
        if self.temp and isfile(self._path):
            os.remove(self._path)
        self.expand_jobs()

    def expand_jobs(self):
        for job in self.jobs[:]:
            flag_match = map(lambda x: "".join(
                x), CmdTemplate.pattern.findall(job.cmd0))
            flag = []
            for f in flag_match:
                if f in self.envs:
                    if f.endswith(".value"):
                        flag.append(f[:-6])
                    flag.append(f)
            flag = sorted(set(flag), key=flag.index)
            if not job.extend and flag:
                job.extend = flag
            job.extend = extends = sorted(
                set([e[:-6] if e.endswith(".value") else e for e in job.extend]), key=flag.index)
            ext_values = [self.envs[e] for e in extends]
            extra_flag = [e for e in flag if e not in job.extend]
            for sub in product(*ext_values):
                if sub:
                    jobname = job.name + "." + ".".join(sub)
                    self.job_set.setdefault(job.name, set()).add(jobname)
                    sub_dict = {extends[n]: i for n, i in enumerate(sub)}
                    extend_detail = dict(zip(extends, sub))
                    for ef in extra_flag:
                        if ef.endswith(".value"):
                            ef_ = ef[:-6]
                            ef_v = self.envs[ef_][extend_detail[ef_]]
                        else:
                            try:
                                n = self.envs[extends[0]].index(sub[0])
                            except AttributeError:
                                n = list(self.envs[extends[0]].keys()).index(
                                    sub[0])
                            idx = n % len(self.envs[ef])
                            ef_v = self.envs[ef][idx]
                        sub_dict.update({ef: ef_v})
                        if ef not in extend_detail:
                            extend_detail[ef] = ef_v
                    if job in self.jobs:
                        self.jobs.remove(job)
                        self.totaljobs.pop(job.name)
                    job_temp = job.copy()
                    job_temp.depends = job.depends.copy()
                    job_temp.extend_detail = extend_detail
                    job_temp.name = job_temp.jobname = jobname
                    job_temp.logfile = join(
                        dirname(job.logfile), jobname + ".log")
                    job_temp.cmd = CmdTemplate(
                        job.cmd).safe_substitute(sub_dict)
                    job_temp.cmd0 = CmdTemplate(
                        job.cmd0).safe_substitute(sub_dict)
                    job_temp.raw_cmd = CmdTemplate(
                        job.raw_cmd).safe_substitute(sub_dict)
                    for dep in job.depends:
                        name, *exts = dep.split(".")
                        exts = [job_temp.extend_detail.get(e, e) for e in exts]
                        if exts:
                            job_temp.depends.remove(dep)
                            dep_name = ".".join([name]+exts)
                            job_temp.depends.add(dep_name)
                            self.job_set.setdefault(name, set()).add(dep_name)
                    self.jobs.append(job_temp)
                    self.totaljobs[job_temp.name] = job_temp

    def expand_orders(self):
        allnames = set([j.name for j in self.jobs])
        for job in self.jobs:
            if not job.depends:
                continue
            for dep in sorted(job.depends):
                if dep in allnames:
                    continue
                if dep in self.job_set:
                    job.depends.remove(dep)
                    job.depends.update(self.job_set[dep])
                    continue
                deps = sorted(n for n in allnames if n.startswith(dep+"."))
                if deps:
                    job.depends.remove(dep)
                    job.depends.update(deps)
                    self.job_set.setdefault(dep, set()).update(deps)
            self.orders.setdefault(job.name, set()).update(job.depends)

    def add_job(self, *args, method=None):
        job = getattr(Job(self.config), method)(self, *args)
        self.totaljobs[job.name] = job

    def add_envs(self, lines=None):
        if not lines or not (re.match("envs?[\s:]", lines[0]) or re.match("vars?[\s:]", lines[0])):
            return
        for line in lines[1:]:
            line = line.split("#")[0]
            k, v = re.split("[=:]", line, 1)
            if "=" in v:
                vd = self._get_value_dict(v.strip())
                self.envs[k.strip()] = vd
                self.envs[k.strip() + ".value"] = list(vd.values())
            else:
                self.envs[k.strip()] = self._get_value_list(v.strip())

    @property
    def alljobnames(self):
        return list(self.totaljobs.keys())

    def _get_value_dict(self, value=""):
        out = OrderedDict()
        if value:
            for kv in self._get_value_list(value):
                kv = kv.split("=")
                k, v = kv[0].strip(), kv[-1].strip()
                if k in out:
                    raise JobError("dup key '{}' in '{}'".format(k, value))
                out[k] = v
        return out

    def _get_value_list(self, value=""):
        out = []
        if value:
            ns = re.split("[\s,]", value)
            out = list(filter(None, ns))
        return [i.strip("$") for i in out]

    def check_orders_name(self):
        for k, v in self.orders.items():
            for n in v:
                if n not in self.totaljobs:
                    raise JobOrderError(
                        "no job named '{}'".format(n))


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
        if not name:
            if self.temp:
                name = basename(self.logdir).split("_")[0]
            else:
                name = basename(self._path)
        if name[0].isdigit():
            name = "job_" + name
        self.name = name

    def parse_jobs(self, start=0, end=None):
        with open(self._path) as fi:
            for n, line in enumerate(fi):
                if n < start-1:
                    continue
                elif end and n > end-1:
                    continue
                line = line.strip().strip("&")
                if line.startswith("#") or not line.strip():
                    continue
                self.add_job(n, line, method="from_cmd")
        if self.temp and isfile(self._path):
            os.remove(self._path)
        self.jobs = list(self.totaljobs.values())
        self.expand_jobs()
