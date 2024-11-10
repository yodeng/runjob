#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
import signal
import getpass
import logging
import argparse
import subprocess

from . import dag
from .job import *
from .utils import *
from .cluster import *
from .context import context
from ._jobsocket import listen_job_status


class RunJob(object):

    def __init__(self, config=None, **kwargs):
        '''
        all attribute of config or kwargs:
            @jobfile <file, list>: required
            @jobname <str>: default: basename(jobfile)
            @mode <str>: default: sge
            @queue <list>: default: all access queue
            @num <int>: default: number of jobs in parallel
            @start <int>: default: 1
            @end <int>: default: None
            @cpu <int>: default: 1
            @memory <int>: default: 1
            @groups <int>: default: 1
            @strict <bool>: default: False
            @force <bool>: default: False
            @logdir <dir>: defalut: "{0}/run*_*_log_dir"
            @workdir <dir>: default: {0}
            @max_check <int>: default: {1}
            @max_submit <int>: default: {2}
            @loglevel <int>: default: None
            @quiet <bool>: default False
            @retry <int>: retry times, default: 0
            @retry_sec <int>: retryivs sec, default: 2
            @sec <int>: submit epoch ivs, default: 2
        '''.format(os.getcwd(), DEFAULT_MAX_CHECK_PER_SEC, DEFAULT_MAX_SUBMIT_PER_SEC)
        self.conf = config = config or context.conf
        for k, v in kwargs.items():
            setattr(self.conf.info.args, k, v)
        self.jobfile = config.jobfile
        if not self.jobfile:
            raise RunJobError("Empty jobs input")
        self.quiet = config.quiet
        self.queue = config.queue
        self.maxjob = config.num
        self.cpu = config.cpu or 1
        self.node = config.node or None
        self.round_node = cycle(
            self.node) if self.node and config.round_node else None
        self.mem = config.memory or 1
        self.groups = config.groups or 1
        self.strict = config.strict or False
        self.workdir = abspath(config.workdir or os.getcwd())
        self.jfile = Shellfile(self.jobfile, mode=config.mode or "sge", name=config.jobname,
                               logdir=config.logdir, workdir=self.workdir, config=config)
        self.logdir = self.jfile.logdir
        self.jpath = self.jfile._path
        self.jfile.parse_jobs(
            start=config.start or 1, end=config.end)
        self.jobs = self.jfile.jobs
        self.mode = self.jfile.mode
        self.name = self.jfile.name
        self.retry = config.retry or 0
        self.retry_sec = config.retry_sec or 2
        self.sec = config.sec or 2
        self._init()
        self.lock = Lock()
        self._status_queue = Queue()
        self._status_socket_file = join(
            self.logdir, ".{}.{}.socket".format(os.getpid(), __package__))

    def _init(self):
        self.totaljobdict = {j.jobname: j for j in self.jobs}
        self.jobnames = [j.name for j in self.jobs]
        self.is_run = False
        self.submited = False
        self.finished = False
        self.signaled = False
        self.err_msg = ""
        self.reseted = False
        self.localprocess = {}
        self.cloudjob = {}
        self.batch_jobid = {}
        self.jobsgraph = dag.DAG()
        self.has_success = []
        self.__add_depency_for_wait()
        self.__group_jobs()
        self.init_callback()
        if self.conf.loglevel:
            self.logger.setLevel(self.conf.loglevel)
        self.conf.logger = self.logger
        self.conf.cloudjob = self.cloudjob
        self.check_rate = Fraction(
            self.conf.max_check or DEFAULT_MAX_CHECK_PER_SEC).limit_denominator()
        self.sub_rate = Fraction(
            self.conf.max_submit or DEFAULT_MAX_SUBMIT_PER_SEC).limit_denominator()
        self.maxjob = int(self.maxjob or len(self.jobs))
        self.jobqueue = JobQueue(maxsize=min(max(self.maxjob, 1), 1000))
        self.init_time_stamp = now()

    def reset(self):
        self.jfile = Shellfile(self.jobfile, mode=self.mode, name=self.name,
                               logdir=self.logdir, workdir=self.workdir)
        self.jfile.parse_jobs(
            start=self.conf.start or 1, end=self.conf.end)
        self.jobs = self.jfile.jobs
        self._init()
        self.reseted = True

    def __add_depency_for_wait(self):
        cur_jobs, dep_jobs = [], []
        for j in self.jobs[:]:
            if j.raw_cmd == "wait":
                if cur_jobs:
                    dep_jobs = cur_jobs[:]
                    cur_jobs = []
            else:
                self.jobsgraph.add_node_if_not_exists(j.jobname)
                if dep_jobs:
                    for dep_j in dep_jobs:
                        self.jobsgraph.add_edge(dep_j.jobname, j.jobname)
                cur_jobs.append(j)

    def __group_jobs(self):
        jobs_groups = []
        jgs = []
        for j in self.jobs[:]:
            if j.raw_cmd == "wait":
                self.jobs.remove(j)
                if jgs:
                    jobs_groups.append(jgs)
                    jgs = []
            else:
                jgs.append(j)
        if jgs:
            jobs_groups.append(jgs)
        for wait_groups in jobs_groups:
            i = 0
            for n, jb in enumerate(wait_groups):
                if jb.groups:
                    if n >= i:
                        self.__make_groups(wait_groups[n:n + jb.groups])
                        i = jb.groups + n
                    else:
                        self.throw('groups conflict in "%s" line number %d: "%s"' % (self.jpath,
                                                                                     jb.linenum, jb.cmd0))
                elif n >= i and (n - i) % self.groups == 0:
                    gs = []
                    for j in wait_groups[n:n + self.groups]:
                        if j.groups:
                            break
                        gs.append(j)
                    self.__make_groups(gs)

    def __make_groups(self, jobs=None):
        if len(jobs) > 1:
            j_header = jobs[0]
            j_header.groups = len(jobs)
            for j in jobs[1:]:
                j_header.raw_cmd += "\n" + j.raw_cmd
                if j in self.jobs:
                    self.jobs.remove(j)
                self.jobsgraph.delete_node_if_exists(j.jobname)
            j_header.raw2cmd()
            self.totaljobdict[j_header.jobname] = j_header

    @property
    def _job2rule(self):
        out = {}
        for k, v in self.jfile.job_set.items():
            for i in v:
                out.setdefault(i, set()).add(k)
        return {k: sorted(v, key=len)[0] for k, v in out.items()}

    def check_already_success(self):
        for job in sorted(self.jobs):
            lf = job.logfile
            job.subtimes = 0
            job.remove_all_stat_files()
            if isfile(lf):
                js = self.jobstatus(job)
                if js != "success":
                    os.remove(lf)
                    job.status = "wait"
                elif hasattr(job, "logcmd") and job.logcmd.strip() != job.raw_cmd.strip():
                    self.logger.info(
                        "job %s status already success, but raw command changed, will re-running", job.jobname)
                    os.remove(lf)
                    job.status = "wait"
                else:
                    if self.conf.force or getattr(job, "force", False):
                        self.logger.info(
                            "job %s status already success, but force to re-running", job.jobname)
                        os.remove(lf)
                        job.status = "wait"
                    else:
                        self.jobsgraph.delete_node_if_exists(job.jobname)
                        self.has_success.append(job)
                        self.jobs.remove(job)
                        job.remove_all_stat_files()
            else:
                job.status = "wait"

    def init_callback(self):
        self.add_init_and_callback(cmd=self.conf._getvalue(
            "init") or self.conf.rget("args", "init"))
        self.add_init_and_callback(cmd=self.conf._getvalue(
            "call_back") or self.conf.rget("args", "call_back"), flag=-1)

    def add_init_and_callback(self, cmd="", name="", flag=0):
        name = name or (flag and "callback" or "init")
        if cmd:
            if name in self.totaljobdict:
                job = self.totaljobdict[name]
                return self.logger.error("job name '%s': ('%s') exists", name, job.cmd0)
            job = Job(self.conf)
            job = job.from_cmd(self.jfile, linenum=-1, cmd=cmd)
            job.to_local(jobname=name, removelog=False)
            self.totaljobdict[name] = job
            if flag == 0:
                self.jobs.insert(0, job)
                edge = (lambda j: (name, j))
            elif flag == -1:
                self.jobs.append(job)
                edge = (lambda j: (j, name))
            else:
                return self.logger.error("init or callback flag error: %s", flag)
            nodes = self.jobsgraph.ind_nodes()
            self.jobsgraph.add_node_if_not_exists(job.jobname)
            for j in nodes:
                self.jobsgraph.add_edge(*edge(j))

    def log_status(self, job):
        name = job.jobname
        if name in self.cloudjob:
            name += " (task-id: {})".format(self.cloudjob[name])
        elif name in self.batch_jobid:
            name += " (job-id: {})".format(self.batch_jobid[name])
        elif name in self.localprocess:
            name += " (pid: {})".format(self.localprocess[name].pid)
        if job.is_fail:
            level = "error"
        elif job.status == "resubmit":
            level = "warning"
        else:
            level = "info"
        if not job.is_wait:
            getattr(self.logger, level)("job %s status %s", name, job.status)

    def jobstatus(self, job):
        jobname = job.jobname
        status = job.status
        logfile = job.logfile
        if self.is_run and job.host == "batchcompute":
            if jobname in self.cloudjob:
                time.sleep(0.1)
                jobid = self.cloudjob[jobname]
                try:
                    j = job.client.get_job(jobid)
                    sta = j.State
                except ClientError as e:  # delete by another process, status Failed
                    self.logger.debug("Job %s not Exists", jobid)
                    self.cloudjob.pop(jobname)
                    sta = "Failed"
                if sta == "Running":
                    status = "run"
                elif sta == "Finished":
                    status = "success"
                elif sta == "Failed":
                    status = "error"
                elif sta == "Stopped":
                    status = "stop"
                elif sta == "Waiting":
                    status = "wait"
        elif self.is_run and not isfile(job.stat_file + ".submit"):
            if isfile(job.stat_file + ".success"):
                status = "success"
            elif isfile(job.stat_file + ".error"):
                status = "error"
            elif isfile(job.stat_file + ".run"):
                if not job.is_end:
                    status = "run"
        elif isfile(logfile):  # local submit or sge submit(not running yet)
            time.sleep(0.1)
            with os.popen('tail -n 1 %s' % logfile) as fi:
                sta = fi.read().strip()
                stal = sta.split()
            if sta:
                if stal[-1].upper() == "SUCCESS":
                    status = "success"
                elif stal[-1].upper() == "ERROR":
                    status = "error"
                elif stal[-1] == "Exiting.":
                    status = "exit"
                elif "RUNNING..." in sta:
                    status = "run"
                # sge submit, but not running
                elif stal[-1] == "submitted" and self.is_run and job.host == "sge":
                    jobid = self.batch_jobid.get(jobname, jobname)
                    try:
                        info = check_output(
                            "qstat -j %s" % jobid, stderr=PIPE, shell=True)
                        info = info.decode().strip().split("\n")[-1]
                        if info.startswith("error") or ("error" in info and "Job is in error" in info):
                            self.logger.debug(
                                "batch job {} (job-id: {}) schedule error".format(jobname, jobid))
                            status = "error"
                    except BlockingIOError as e:
                        raise e
                    except Exception as e:
                        self.logger.warning(str(e))
                        status = "error"
                # slurm submit, but not running
                elif re.search("Submitted batch job \d+", sta) and self.is_run and job.host == "slurm":
                    pass
                    '''
                    jobid = self.batch_jobid.get(jobname, jobname)
                    try:
                        info = check_output(
                            "scontrol show job %s" % jobid, stderr=PIPE, shell=True)
                        if "Command=(null)" in info.decode():
                            self.logger.debug(
                                "batch job {} (job-id: {}) schedule error".format(jobname, jobid))
                            status = "error"
                    except BlockingIOError as e:
                        raise e
                    except Exception as e:
                        self.logger.warning(str(e))
                        status = "error"
                    '''
                else:
                    status = "run"
            else:
                status = "run"
            if self.is_run and job.host.startswith("local") and jobname not in self.localprocess:
                status = job.status
            if job.host.startswith("local") and jobname in self.localprocess:
                ps = self.localprocess[jobname].poll()
                if ps and ps < 0:
                    status = "kill"
            if not self.is_run and status == "success":
                job.logcmd = ""
                with open(logfile) as fi:
                    for line in fi:
                        if not line.strip():
                            continue
                        if line.startswith("["):
                            break
                        job.logcmd += line
                job.logcmd = job.logcmd.strip()
        if status == "run" and job.is_end and self.is_run and self.submited:
            status = job.status
        self.logger.debug("job %s status %s", jobname, status)
        if status != job.status and self.is_run and self.submited and job.submited:
            if status == "run":
                job.run_time = now()
            job.set_status(status)
            if not self.signaled:
                self.log_status(job)
            if job.host == "batchcompute":
                with open(logfile, "a") as fo:
                    fo.write("[%s] %s\n" % (
                        datetime.today().strftime("%F %X"), job.status.upper()))
        return status

    def get_status(self):
        while not self.finished:
            name, js = self._status_queue.get()
            if name not in self.totaljobdict:
                continue
            jb = self.totaljobdict[name]
            if js != jb.status:
                if status == "run":
                    jb.run_time = now()
                jb.set_status(status)
                if not self.signaled:
                    self.log_status(jb)
            self.adjust_jobsgraph(jb, js)

    def send_status(self, name, status):
        self._status_queue.put((name, status))

    def set_rate(self, check_rate=0, sub_rate=0):
        if check_rate:
            self.check_rate = Fraction(check_rate).limit_denominator()
        if sub_rate:
            self.sub_rate = Fraction(sub_rate).limit_denominator()

    def _list_check_sge(self, period=5, sleep=10):
        rate_limiter = RateLimiter(max_calls=1, period=period)
        time.sleep(sleep)

        def _deal_check(self, jb):
            if self.is_run and not jb.is_end and isfile(jb.stat_file + ".run"):
                time.sleep(1)
                _ = self.jobstatus(jb)
                jb.set_kill()
                self.log_status(jb)

        while not self.finished:
            for jb in self.jobqueue.queue:
                jobname = jb.jobname
                if jb.status != "run":
                    continue
                with rate_limiter:
                    if jobname in self.localprocess:
                        ps = self.localprocess[jobname].poll()
                        if ps and ps < 0:
                            _deal_check(self, jb)
                    elif jobname in self.batch_jobid:
                        jobid = self.batch_jobid.get(jobname)
                        if jobid and jobid.isdigit():
                            if jb.host == "sge":
                                try:
                                    check_output(
                                        ["qstat", "-j", jobid], stderr=-3)
                                except Exception as err:
                                    self.logger.debug(err)
                                    _deal_check(self, jb)
                            elif jb.host == "slurm":
                                pass
            time.sleep(sleep)

    def jobcheck(self):
        RunThread(self._jobcheck).start()
        RunThread(self._list_check_sge).start()

    def _job_status_server(self):
        RunThread(listen_job_status, self._status_socket_file,
                  self._status_queue).start()

    def _jobcheck(self):
        if self.mode == "batchcompute":
            self.set_rate(check_rate=1)
        rate_limiter = RateLimiter(
            max_calls=self.check_rate.numerator, period=self.check_rate.denominator)
        while not self.finished:
            for jb in self.jobqueue.queue:
                with rate_limiter:
                    try:
                        js = self.jobstatus(jb)
                    except BlockingIOError as e:
                        self.logger.warning(
                            "check job status blocking: %s, %s", jb.name, e)
                        continue
                    except Exception as e:
                        self.logger.error(
                            "check job status error: %s", jb.name)
                        self.logger.exception(e)
                        continue
                    self.adjust_jobsgraph(jb, js)

    def adjust_jobsgraph(self, jb, js):
        if js == "success":
            self.deletejob(jb)
            self.jobqueue.get(jb)
            self.jobsgraph.delete_node_if_exists(jb.jobname)
        elif js == "error":
            self.deletejob(jb)
            if not jb.timeout:
                if jb.subtimes >= self.times + 1:
                    if self.strict:
                        self.throw("Error jobs return (submit %d times), %s" % (
                            jb.subtimes, jb.logfile))
                    self.jobqueue.get(jb)
                    self.jobsgraph.delete_node_if_exists(
                        jb.jobname)
                else:
                    self.jobqueue.get(jb)
                    self.submit(jb)
            else:
                if jb.max_timeout_retry > 0:
                    self.jobqueue.get(jb)
                    self.submit(jb)
                    jb.max_timeout_retry -= 1
                else:
                    if self.strict:
                        self.throw("Error jobs return (submit %d times), %s" % (
                            jb.subtimes, jb.logfile))
                    self.jobqueue.get(jb)
                    self.jobsgraph.delete_node_if_exists(
                        jb.jobname)
        elif js in ["exit", "kill"]:
            self.deletejob(jb)
            self.jobqueue.get(jb)
            self.jobsgraph.delete_node_if_exists(jb.jobname)
            if self.strict:
                self.throw("Error job: %s, exit" % jb.jobname)
        elif js in ["run", "submit", "resubmit"]:
            _now = now()
            if _now - jb.submit_time > jb.max_wait_sec or \
                    js == "run" and _now - jb.run_time > jb.max_run_sec or \
                    js != "run" and _now - jb.submit_time > jb.max_queue_sec:
                self.deletejob(jb)
                jb.timeout = True
                jb.status = "error"
                jb.log_to_file("Timeout ERROR")
                self.logger.error(
                    "job %s status timeout %s", jb.jobname, jb.status)

    def qdel(self, jobname=""):
        self._qdel(jobname)

    def call_without_exception(self, cmd, verbose=False, run=True, daemon=False, timeout=3):
        if not run:
            return
        func_name = "Popen" if daemon else "call"
        if verbose:
            self.logger.info(f"{func_name}: {cmd}")
        else:
            self.logger.debug(f"{func_name}: {cmd}")
        try:
            getattr(subprocess, func_name)(cmd, shell=isinstance(cmd, str),
                                           stdout=not verbose and -3 or None, stderr=-2, timeout=timeout)
        except:
            pass

    # Override these methods to implement other subclass
    def _qdel(self, jobname=""):
        if jobname in self.batch_jobid:
            jobid = self.batch_jobid.get(jobname, jobname)
            jb = self.totaljobdict[jobname]
            if jb.host == "sge":
                self.call_without_exception(["qdel", jobid])
            elif jb.host == "slurm":
                self.call_without_exception(["scancel", jobid])
            self.batch_jobid.pop(jobname, None)
        else:
            sge_jobids, slurm_jobids = set(), set()
            for name, jid in self.batch_jobid.items():
                host = self.totaljobdict[name].host
                if host == "sge":
                    sge_jobids.add(jid)
                elif host == "slurm":
                    slurm_jobids.add(jid)
            if sge_jobids:
                # self.call_without_exception(['qdel', "*_%d*" % os.getpid()])
                self.call_without_exception(['qdel',] + list(sge_jobids))
            if slurm_jobids:
                self.call_without_exception(["scancel",] + list(slurm_jobids))
            self.batch_jobid.clear()

    def deletejob(self, jb=None):
        if jb:
            if jb.jobname in self.localprocess:
                p = self.localprocess.pop(jb.jobname)
                if p.poll() is None:
                    terminate_process(p.pid)
                p.wait()
            elif isfile(jb.stat_file + ".success") or isfile(jb.stat_file + ".error"):
                pass
            elif jb.host in ["sge", "slurm"]:
                self.qdel(jobname=jb.jobname)
            jb.remove_all_stat_files()
        else:
            self.qdel()
            for jb in self.jobqueue.queue:
                jb.remove_all_stat_files()

    def submit(self, job):
        if not self.is_run or job.do_not_submit:
            return
        logfile = job.logfile
        self.jobqueue.put(job, block=True, timeout=1080000)
        job.timeout = False
        job.submit_time = now()
        with open(logfile, "a") as logcmd:
            if job.subtimes == 0:
                logcmd.write(textwrap.dedent(job.raw_cmd).strip() + "\n")
                job.set_status("submit")
            elif job.subtimes > 0:
                logcmd.write(style("\n-------- retry --------\n",
                             fore="red", mode="bold") + job.raw_cmd + "\n")
                job.set_status("resubmit")
            logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
            logcmd.flush()
            if job.host is not None and job.host in ["localhost", "local"]:
                job.raw2cmd(job.subtimes and abs(
                    self.retry_sec) or 0, groupsh=True)
                cmd = "(echo 'Your job (\"%s\") has been submitted in localhost') && " % job.name + job.cmd
                mkdir(job.workdir)
                touch(job.stat_file + ".submit")
                with tmp_chdir(job.workdir):
                    p = Popen(cmd, shell=True, stdout=logcmd,
                              stderr=logcmd, env=os.environ, cwd=job.workdir)
                self.localprocess[job.name] = p
            elif job.host == "sge":
                job.raw2cmd(job.subtimes and abs(
                    self.retry_sec) or 0, groupsh=True)
                jobcpu = job.cpu or self.cpu
                jobmem = job.mem or self.mem
                job.update_queue(self.queue)
                cmd = job.qsub_cmd(jobmem, jobcpu)
                if job.queue:
                    cmd += " -q " + " -q ".join(job.queue)
                cmd += self.node_select(job)
                mkdir(job.workdir)
                touch(job.stat_file + ".submit")
                job.batch_sub_cmd = cmd.replace("`", "\`")
                with tmp_chdir(job.workdir):
                    jobid, output = self.batch_sub(job)
                self.batch_jobid[job.jobname] = jobid
                logcmd.write(output)
            elif job.host == "slurm":
                job.raw2cmd(job.subtimes and abs(
                    self.retry_sec) or 0, groupsh=True)
                jobcpu = job.cpu or self.cpu
                jobmem = job.mem or self.mem
                headers = job.sbatch_header(jobmem, jobcpu)
                job.update_queue(self.queue)
                if not job.queue:
                    with os.popen("sinfo -h | awk '{print $1}'") as fi:
                        job.queue = set([i.strip("*")
                                        for i in fi.read().split()])
                headers += "#SBATCH --partition={0}\n".format(
                    ",".join(sorted(job.queue)))
                headers += self.node_select(job)
                cmd = headers+"\n"+job.cmd
                job.batch_sub_cmd = cmd
                mkdir(job.workdir)
                touch(job.stat_file + ".submit")
                with tmp_chdir(job.workdir):
                    jobid, output = self.batch_sub(job, mode="slurm")
                self.batch_jobid[job.jobname] = jobid
                logcmd.write(output)
            elif job.host == "batchcompute":
                jobcpu = job.cpu or self.cpu
                jobmem = job.mem or self.mem
                c = Cluster(config=self.conf)
                c.AddClusterMount()
                task = Task(c)
                task.AddOneTask(
                    job=job, outdir=self.conf.args.out_maping)
                if job.out_maping:
                    task.modifyTaskOutMapping(job=job, mapping=job.out_maping)
                task.Submit()
                info = "Your job (%s) has been submitted in batchcompute (%s) %d times\n" % (
                    task.name, task.id, job.subtimes + 1)
                logcmd.write(info)
                self.cloudjob[task.name] = task.id
            self.log_status(job)
            job.subtimes += 1
            self.logger.debug("%s job submit %s times", job.name, job.subtimes)
        job.submited = True
        self.submited = True

    def node_select(self, job):
        cmd_or_header = ""
        nodelist = []
        if job.host == "sge":
            if job.node:
                nodelist = sorted(set(job.node))
            elif self.node and self.round_node:
                nodelist = [next(self.round_node), ]
            elif self.node and not self.round_node:
                nodelist = sorted(set(self.node))
            if nodelist:
                cmd_or_header += " " + \
                    " ".join(["-l hostname={}".format(node)
                             for node in nodelist])
        elif job.host == "slurm":
            if job.node:
                nodelist = sorted(set(job.node))
            elif self.node and self.round_node:
                nodelist = [next(self.round_node), ]
            elif self.node and not self.round_node:
                nodelist = sorted(set(self.node))
            if nodelist:
                cmd_or_header += "#SBATCH --nodelist={0}\n".format(
                    ",".join(nodelist))
                cmd_or_header += "#SBATCH --nodes={}\n".format(len(nodelist))
            else:
                cmd_or_header += "#SBATCH --nodes=1\n"
        return cmd_or_header

    def batch_sub(self, job, mode="sge"):
        if mode == "sge":
            p = Popen(job.batch_sub_cmd, stderr=PIPE, text=True,
                      stdout=PIPE, shell=True, cwd=job.workdir or self.workdir)
            stdout, stderr = p.communicate()
            output = stdout + stderr
            match = QSUB_JOB_ID_DECODER.search(output)
        elif mode == "slurm":
            p = subprocess.run("sbatch", input=job.batch_sub_cmd, stderr=PIPE,
                               stdout=PIPE, text=True, shell=True, cwd=job.workdir or self.workdir)
            output = p.stdout + p.stderr
            match = SBATCH_JOB_ID_DECODER.search(output)
        if match:
            jobid = match.group(1)
        else:
            self.throw(output)
        return jobid, output

    def run(self):
        if self.is_run:
            return self.logger.warning("not allowed for job has run")
        elif len(self.jobsgraph.graph) == 0:
            return self.logger.warning("no jobs produced in '%s'", self.jobfile)
        elif self.conf.rget("args", "dag_extend"):
            print(self.jobsgraph.dot(self._job2rule))
            sys.exit()
        elif self.conf.rget("args", "dag"):
            print(self.jobsgraph.to_rulegraph(self._job2rule))
            sys.exit()
        self.run_time_stamp = now()
        self.times = max(0, self.retry)
        self.retry_sec = max(self.retry_sec, 0)
        self.logger.info("Total jobs to submit: %s" %
                         ", ".join([j.name for j in sorted(self.jobs)]))
        mkdir(self.logdir, self.workdir)
        self.logger.info("All logs can be found in %s directory", self.logdir)
        self.check_already_success()
        for jn in self.has_success:
            self.logger.info("job %s status already success", jn.name)
        self.is_run = True
        if len(self.jobsgraph.graph) == 0:
            return self.logger.warning("no jobs need to submit")
        if not self.reseted:
            self.cleanup()
        if self.mode == "batchcompute":
            access_key_id = self.conf.args.access_key_id or self.conf.access_key_id
            access_key_secret = self.conf.args.access_key_secret or self.conf.access_key_secret
            region = REGION.get(self.conf.args.region.upper(), CN_BEIJING)
            client = Client(region, access_key_id, access_key_secret)
            quotas = client.get_quotas().AvailableClusterInstanceType
            cfg_path = join(dirname(__file__), "ins_type.json")
            with open(cfg_path) as fi:
                self.conf.it_conf = json.load(fi)
            availableTypes = [i for i in quotas if i in self.conf.it_conf]
            self.conf.availableTypes = sorted(availableTypes, key=lambda x: (
                self.conf.it_conf[x]["cpu"], self.conf.it_conf[x]["memory"]))
            self.conf.client = self.client = client
        sub_rate_limiter = RateLimiter(
            max_calls=self.sub_rate.numerator, period=self.sub_rate.denominator)
        self.jobcheck()
        # self._job_status_server()
        while True:
            subjobs = self.jobsgraph.ind_nodes()
            if len(subjobs) == 0:
                break
            for jb in self.pending_jobs(*subjobs):
                with sub_rate_limiter:
                    self.submit(jb)
            time.sleep(self.sec)
        self.safe_exit()
        if not self.is_success:
            raise JobFailedError(jobs=self.fail_jobs)

    def pending_jobs(self, *names):
        jobs = []
        for j in sorted(names, key=sort_by):
            jb = self.totaljobdict[j]
            if jb not in self.jobqueue:
                jobs.append(jb)
        return jobs

    @property
    def logger(self):
        if self.quiet:
            logging.disable()
        return context.log

    def _clean_jobs(self):
        if self.mode in ["sge", "slurm"]:
            try:
                self.deletejob()
            except:
                self.qdel()
            for jb in self.jobqueue.queue:
                jb.remove_all_stat_files()
                jb.set_kill()
        elif self.mode == "batchcompute":
            user = getpass.getuser()
            for jb in self.jobqueue.queue:
                jobname = jb.name
                try:
                    jobid = self.cloudjob.get(jobname, "")
                    j = self.client.get_job(jobid)
                except ClientError as e:
                    if e.status == 404:
                        self.logger.error("Invalid JobId %s", jobid)
                        continue
                except:
                    continue
                if j.Name.startswith(user):
                    if j.State not in ["Stopped", "Failed", "Finished"]:
                        self.client.stop_job(jobid)
                    self.client.delete_job(jobid)
                    self.logger.info("Delete job %s done", j.Name)
                else:
                    self.logger.error(
                        "Delete job error, you have no assess with job %s", j.Name)
        for j, p in self.localprocess.copy().items():
            jb = self.totaljobdict[j]
            self.deletejob(jb)
            jb.set_kill()

    def throw(self, msg=""):
        self.err_msg = msg
        if self.is_run:
            self.safe_exit()
        if threading.current_thread().name == 'MainThread':
            raise RunJobError(self.err_msg)
        else:
            os.kill(os.getpid(), signal.SIGUSR1)  # threading exit

    def writestates(self, outstat):
        summary = {j.name: self.totaljobdict[j.name].status for j in self.jobs}
        elaps = now() - self.run_time_stamp
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(summary.values()))) + "\n")
            fo.write("# Detail:\n")
            sumout = {}
            for k, v in summary.items():
                sumout.setdefault(v, []).append(k)
            for k, v in sorted(sumout.items()):
                fo.write(
                    k + " : " + ", ".join(sorted(v, key=lambda x: (len(x), x))) + "\n")
            fo.write("\n# Time Elapse: %s\n" % seconds2human(elaps))

    def cleanup(self):
        ParseSingal(obj=self).start()

    def safe_exit(self):
        with self.lock:
            self._clean_jobs()
            if self.err_msg:
                self.logger.error(self.err_msg)
            self.sumstatus()
        self.__del__()

    @property
    def is_success(self):
        return all(j.is_success for j in self.jobs)

    @property
    def fail_jobs(self):
        return [j for j in self.jobs if j.is_fail]

    def sumstatus(self):
        if not hasattr(self, "jobs") or not len(self.jobs) or self.finished:
            return
        fail_jobs = len(self.fail_jobs)
        suc_jobs = sum(j.is_success for j in self.jobs)
        wt_jobs = sum(j.is_wait for j in self.jobs)
        total_jobs = len(self.jobs) + len(self.has_success)
        sub_jobs = len(self.jobs) - wt_jobs
        sum_info = "All jobs (total: %d, submited: %d, success: %d, fail: %d, wait: %d) " % (
            total_jobs, sub_jobs, suc_jobs, fail_jobs, wt_jobs)
        if hasattr(self, "jfile") and not self.jfile.temp:
            sum_info += "in file '%s' " % abspath(self.jpath)
        self.writestates(join(
            self.logdir, "job_%s.status.txt" % self.name))
        job_counter = str(dict(Counter([j.status for j in self.jobs])))
        self.finished = True
        if self.is_success:
            sum_info += "finished successfully."
            self.logger.info(sum_info)
            self.logger.info(job_counter)
        else:
            sum_info += "finished, but there are unsuccessful job."
            self.logger.error(sum_info)
            self.logger.error(job_counter)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            raise exc_type(exc_val)

    def __del__(self):
        try:
            os.remove(self._status_socket_file)
        except:
            pass


class RunFlow(RunJob):

    def __init__(self, config=None, **kwargs):
        '''
        all attribute of config or kwargs:
            @jobfile <file, list>: required
            @mode <str>: default: sge
            @queue <list>: default: all access queue
            @cpu <int>: default: 1
            @memory <int>: default: 1
            @num <int>: default: number of jobs in parallel
            @start <int>: default: 1
            @end <int>: default: None
            @strict <bool>: default: False
            @force <bool>: default: False
            @max_check <int>: default: {0}
            @max_submit <int>: default: {1}
            @loglevel <int>: default: None
            @quiet <bool>: default False
            @retry <int>: retry times, default: 0
            @retry_sec <int>: retryivs sec, default: 2
            @sec <int>: submit epoch ivs, default: 2
        '''.format(DEFAULT_MAX_CHECK_PER_SEC, DEFAULT_MAX_SUBMIT_PER_SEC)
        self.conf = config = config or context.conf
        for k, v in kwargs.items():
            setattr(self.conf.info.args, k, v)
        self.quiet = config.quiet
        self.jobfile = config.jobfile
        self.queue = config.queue
        self.cpu = config.cpu or 1
        self.node = config.node or None
        self.round_node = cycle(
            self.node) if self.node and config.round_node else None
        self.mem = config.memory or 1
        self.maxjob = config.num
        self.strict = config.strict or False
        self.workdir = config.workdir or os.getcwd()
        self.jfile = jfile = Jobfile(
            self.jobfile, mode=config.mode or default_backend(), config=config)
        self.jpath = self.jfile._path
        self.mode = jfile.mode
        self.name = os.getpid()
        self.jfile.parse_jobs(
            config.injname, config.start or 1, config.end)
        self.jobs = self.jfile.jobs
        self.logdir = jfile.logdir
        self.retry = config.retry or 0
        self.retry_sec = config.retry_sec or 2
        self.sec = config.sec or 2
        self._init()
        self.lock = Lock()

    def _init(self):
        self.jobnames = [j.name for j in self.jobs]
        self.totaljobdict = {name: jf for name,
                             jf in self.jfile.totaljobs.items()}
        self.jfile.parse_orders()
        self.orders = self.jfile.orders
        self.is_run = False
        self.submited = False
        self.finished = False
        self.signaled = False
        self.err_msg = ""
        self.reseted = False
        self.localprocess = {}
        self.cloudjob = {}
        self.jobsgraph = dag.DAG()
        self.has_success = []
        self.__create_graph()
        if self.conf.loglevel:
            self.logger.setLevel(self.conf.loglevel)
        self.check_rate = Fraction(
            self.conf.max_check or DEFAULT_MAX_CHECK_PER_SEC).limit_denominator()
        self.sub_rate = Fraction(
            self.conf.max_submit or DEFAULT_MAX_SUBMIT_PER_SEC).limit_denominator()
        self.batch_jobid = {}
        self.maxjob = self.maxjob or len(self.jobs)
        self.jobqueue = JobQueue(maxsize=min(max(self.maxjob, 1), 1000))

    def reset(self):
        self.jfile = jfile = Jobfile(self.jobfile, mode=self.mode)
        self.jfile.parse_jobs(
            self.conf.injname, self.conf.start or 1, self.conf.end)
        self.jobs = self.jfile.jobs
        self._init()
        self.reseted = True

    def __create_graph(self):
        for k, v in self.orders.items():
            self.jobsgraph.add_node_if_not_exists(k)
            for i in v:
                self.jobsgraph.add_node_if_not_exists(i)
                self.jobsgraph.add_edge(i, k)
        for jb in self.jobs:
            self.jobsgraph.add_node_if_not_exists(jb.name)
        for jn in self.jobsgraph.all_nodes.copy():
            if jn not in self.jobnames:
                self.jobsgraph.delete_node(jn)
        if len(self.jfile.alljobnames) != len(set(self.jfile.alljobnames)):
            names = [i for i, j in Counter(
                self.jfile.alljobnames).items() if j > 1]
            self.throw("duplicate job name: %s" % " ".join(names))
