#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
import signal
import getpass
import logging
import argparse

from . import dag
from .job import *
from .utils import *
from .cluster import *
from .parser import runsge_parser
from .config import load_config, print_config


class RunJob(object):

    def __init__(self, config=None, **kwargs):
        '''
        all attribute of config or kwargs:
            @jobfile <file, list>: required
            @jobname <str>: default: basename(jobfile)
            @mode <str>: default: sge
            @queue <list>: default: all access queue
            @num <int>: default: total jobs
            @startline <int>: default: 1
            @endline <int>: default: None
            @cpu <int>: default: 1
            @memory <int>: default: 1
            @groups <int>: default: 1
            @strict <bool>: default: False
            @force <bool>: default: False
            @logdir <dir>: defalut: "%s/run*_*_log_dir"
            @workdir <dir>: default: os.getcwd()
            @max_check <int>: default: 3
            @max_submit <int>: default: 30
            @loglevel <int>: default: None
            @quiet <bool>: default False
            @retry <int>: retry times, default: 0
            @retry_ivs <int>: retryivs sec, default: 2
            @sec <int>: submit epoch ivs, default: 2
        ''' % os.getcwd()
        self.conf = config = config or load_config()
        for k, v in kwargs.items():
            setattr(self.conf.info.args, k, v)
        self.jobfile = config.jobfile
        if not self.jobfile:
            raise QsubError("Empty jobs input")
        self.quiet = config.quiet
        self.queue = config.queue
        self.maxjob = config.num
        self.cpu = config.cpu or 1
        self.mem = config.memory or 1
        self.groups = config.groups or 1
        self.strict = config.strict or False
        self.workdir = abspath(config.workdir or os.getcwd())
        self.jfile = Shellfile(self.jobfile, mode=config.mode or "sge", name=config.jobname,
                               logdir=config.logdir, workdir=self.workdir, config=config)
        self.logdir = self.jfile.logdir
        self.jpath = self.jfile._path
        self.jfile.parse_jobs(
            start=config.startline or 1, end=config.endline)
        self.jobs = self.jfile.jobs
        self.mode = self.jfile.mode
        self.name = self.jfile.name
        self.retry = config.retry or 0
        self.retry_ivs = config.retry_ivs or 2
        self.sec = config.sec or 2
        self._init()
        self.lock = Lock()

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
        self.sge_jobid = {}
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
            self.conf.max_check or 3).limit_denominator()
        self.sub_rate = Fraction(
            self.conf.max_submit or 30).limit_denominator()
        self.maxjob = int(self.maxjob or len(self.jobs))
        self.jobqueue = JobQueue(maxsize=min(max(self.maxjob, 1), 1000))
        self.init_time_stamp = now()

    def reset(self):
        self.jfile = Shellfile(self.jobfile, mode=self.mode, name=self.name,
                               logdir=self.logdir, workdir=self.workdir)
        self.jfile.parse_jobs(
            start=self.conf.startline or 1, end=self.conf.endline)
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
                        self.__make_groups(wait_groups[n:n+jb.groups])
                        i = jb.groups+n
                    else:
                        self.throw('groups conflict in "%s" line number %d: "%s"' % (self.jpath,
                                                                                     jb.linenum, jb.cmd0))
                elif n >= i and (n-i) % self.groups == 0:
                    gs = []
                    for j in wait_groups[n:n+self.groups]:
                        if j.groups:
                            break
                        gs.append(j)
                    self.__make_groups(gs)

    def __make_groups(self, jobs=None):
        if len(jobs) > 1:
            j_header = jobs[0]
            for j in jobs[1:]:
                j_header.raw_cmd += "\n" + j.raw_cmd
                if j in self.jobs:
                    self.jobs.remove(j)
                self.jobsgraph.delete_node_if_exists(j.jobname)
            j_header.raw2cmd()
            self.totaljobdict[j_header.jobname] = j_header

    @property
    def _shrink_jobnames(self):
        out = {}
        for k, v in self.jfile.job_set.items():
            for i in v:
                out.setdefault(i, set()).add(k)
        return {k: sorted(v, key=len)[0] for k, v in out.items()}

    def _shrink_graph(self):
        graph = self.jobsgraph.copy()
        shrink_name = self._shrink_jobnames
        for job in self.jobs:
            name = job.jobname
            if name != shrink_name.get(name, name):
                graph.rename_node(name, shrink_name[name])
        return graph

    def check_already_success(self):
        for job in self.jobs[:]:
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
        self.add_init(self.conf.rget("args", "init"))
        self.add_callback(self.conf.rget("args", "call_back"))

    def add_init(self, init=""):
        if init:
            if "init" in self.totaljobdict:
                job = self.totaljobdict["init"]
                return self.logger.error("init '%s' exists", job.raw_cmd)
            job = Job(self.conf)
            job = job.from_cmd(self.jfile, linenum=-1, cmd=init)
            job.to_local(jobname="init", removelog=False)
            self.totaljobdict["init"] = job
            self.jobs.insert(0, job)
            ind_nodes = self.jobsgraph.ind_nodes()
            self.jobsgraph.add_node_if_not_exists(job.jobname)
            for j in ind_nodes:
                self.jobsgraph.add_edge("init", j)

    def add_callback(self, callback=""):
        if callback:
            if "callback" in self.totaljobdict:
                job = self.totaljobdict["callback"]
                return self.logger.error("callback '%s' exists", job.raw_cmd)
            job = Job(self.conf)
            job = job.from_cmd(self.jfile, linenum=-1, cmd=callback)
            job.to_local(jobname="callback", removelog=False)
            self.totaljobdict["callback"] = job
            self.jobs.append(job)
            end_nodes = self.jobsgraph.end_nodes()
            self.jobsgraph.add_node_if_not_exists(job.jobname)
            for j in end_nodes:
                self.jobsgraph.add_edge(j, "callback")

    def log_status(self, job):
        name = job.jobname
        if name in self.cloudjob:
            name += " (task-id: {})".format(self.cloudjob[name])
        elif name in self.sge_jobid:
            name += " (job-id: {})".format(self.sge_jobid[name])
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
        elif job.host and job.host == "sge" and self.is_run and not isfile(job.stat_file+".submit"):
            if isfile(job.stat_file+".success"):
                status = "success"
            elif isfile(job.stat_file+".error"):
                status = "error"
            elif isfile(job.stat_file+".run"):
                if not job.is_end:
                    status = "run"
        elif isfile(logfile):  # local submit or sge submit(not running yet)
            with os.popen('tail -n 1 %s' % logfile) as fi:
                sta = fi.read().strip()
                stal = sta.split()
            if sta:
                if stal[-1] == "SUCCESS":
                    status = "success"
                elif stal[-1] == "ERROR":
                    status = "error"
                elif stal[-1] == "Exiting.":
                    status = "exit"
                elif "RUNNING..." in sta:
                    status = "run"
                # sge submit, but not running
                elif stal[-1] == "submitted" and self.is_run and job.host == "sge":
                    jobid = self.sge_jobid.get(jobname, jobname)
                    try:
                        info = check_output(
                            "qstat -j %s" % jobid, stderr=PIPE, shell=True)
                        info = info.decode().strip().split("\n")[-1]
                        if info.startswith("error") or ("error" in info and "Job is in error" in info):
                            status = "error"
                    except:
                        status = "error"
                else:
                    status = "run"
            else:
                status = "run"
            if self.is_run and job.host.startswith("local") and jobname not in self.localprocess:
                status = job.status
            if job.host.startswith("local") and hasattr(self, "localprocess") and jobname in self.localprocess:
                ret = self.localprocess[jobname].poll()
                if ret and ret < 0:
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
        if status != job.status and self.is_run and self.submited:
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

    def set_rate(self, check_rate=0, sub_rate=0):
        if check_rate:
            self.check_rate = Fraction(check_rate).limit_denominator()
        if sub_rate:
            self.sub_rate = Fraction(sub_rate).limit_denominator()

    def _list_check_sge(self, period=5, sleep=10):
        rate_limiter = RateLimiter(max_calls=1, period=period)
        time.sleep(5)
        while not self.finished:
            for jb in self.jobqueue.queue:
                jobname = jb.jobname
                if jb.host != "sge" or jobname not in self.sge_jobid or jb.status != "run":
                    continue
                with rate_limiter:
                    jobid = self.sge_jobid.get(jobname)
                    if jobid and jobid.isdigit():
                        try:
                            _ = check_output(
                                ["qstat",  "-j", jobid], stderr=PIPE)
                        except Exception as err:
                            self.logger.debug(err)
                            if self.is_run and not jb.is_end and isfile(jb.stat_file+".run"):
                                time.sleep(period)
                                _ = self.jobstatus(jb)
                                jb.set_kill()
                                self.log_status(jb)
            time.sleep(sleep)

    def jobcheck(self):
        RunThread(self._jobcheck).start()
        RunThread(self._list_check_sge).start()

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
                    except Exception as e:
                        self.logger.error(
                            "check job status error: %s", jb.name)
                        self.logger.exception(e)
                        continue
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

    def qdel(self, name="", jobname=""):
        self._qdel(name=name, jobname=jobname)

    # Override these methods to implement other subclass
    def _qdel(self, name="", jobname=""):
        if name:
            call_cmd(['qdel', "*_%d*" % os.getpid()])
            self.sge_jobid.clear()
        if jobname:
            jobid = self.sge_jobid.get(jobname, jobname)
            call_cmd(["qdel", jobid])
            if jobname in self.sge_jobid:
                self.sge_jobid.pop(jobname)

    def deletejob(self, jb=None, name=""):
        if name:
            self.qdel(name=name)
            for jb in self.jobqueue.queue:
                jb.remove_all_stat_files()
        else:
            if jb.jobname in self.localprocess:
                p = self.localprocess.pop(jb.jobname)
                if p.poll() is None:
                    terminate_process(p.pid)
                p.wait()
            if jb.host == "sge":
                self.qdel(jobname=jb.jobname)
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
                logcmd.write(job.raw_cmd+"\n")
                job.set_status("submit")
            elif job.subtimes > 0:
                logcmd.write(style("\n-------- retry --------\n",
                             fore="red", mode="bold") + job.raw_cmd+"\n")
                job.set_status("resubmit")
            logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
            logcmd.flush()
            if job.host is not None and job.host in ["localhost", "local"]:
                job.raw2cmd(job.subtimes and abs(self.retry_ivs) or 0)
                cmd = "(echo 'Your job (\"%s\") has been submitted in localhost') && " % job.name + job.cmd
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                mkdir(job.workdir)
                p = Popen(cmd, shell=True, stdout=logcmd,
                          stderr=logcmd, env=os.environ, cwd=job.workdir)
                self.localprocess[job.name] = p
            elif job.host == "sge":
                job.raw2cmd(job.subtimes and abs(self.retry_ivs) or 0)
                touch(job.stat_file + ".submit")
                jobcpu = job.cpu or self.cpu
                jobmem = job.mem or self.mem
                job.update_queue(self.queue)
                cmd = job.qsub_cmd(jobmem, jobcpu)
                if job.queue:
                    cmd += " -q " + " -q ".join(job.queue)
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                sgeid, output = self.sge_qsub(cmd, wd=job.workdir)
                self.sge_jobid[job.jobname] = sgeid
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
                    task.name, task.id, job.subtimes+1)
                logcmd.write(info)
                self.cloudjob[task.name] = task.id
            self.log_status(job)
            self.logger.debug("%s job submit %s times", job.name, job.subtimes)
            job.subtimes += 1
        self.submited = True

    def sge_qsub(self, cmd, wd=None):
        p = Popen(cmd.replace("`", "\`"), stderr=PIPE,
                  stdout=PIPE, shell=True, cwd=wd or self.workdir)
        stdout, stderr = p.communicate()
        output = stdout + stderr
        match = QSUB_JOB_ID_DECODER.search(output.decode())
        if match:
            jobid = match.group(1)
        else:
            self.throw(output.decode())
        return jobid, output.decode()

    def run(self):
        if self.is_run:
            self.logger.warning("not allowed for job has run")
            return
        self.times = max(0, self.retry)
        self.retry_ivs = max(self.retry_ivs, 0)
        self.run_time_stamp = now()
        self.check_already_success()
        if self.conf.rget("args", "dot"):
            print(self.jobsgraph.dot(self._shrink_jobnames))
            sys.exit()
        if self.conf.rget("args", "dot_shrinked"):
            print(self._shrink_graph())
            sys.exit()
        self.is_run = True
        self.logger.info("Total jobs to submit: %s" %
                         ", ".join([j.name for j in sorted(self.jobs+self.has_success)]))
        mkdir(self.logdir, self.workdir)
        self.logger.info("All logs can be found in %s directory", self.logdir)
        for jn in self.has_success:
            self.logger.info("job %s status already success", jn.name)
        if len(self.jobsgraph.graph) == 0:
            self.logger.warning("no jobs need to submit")
            return
        if not self.reseted:
            self.clean_resource()
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
        for j in sorted(names):
            jb = self.totaljobdict[j]
            if jb not in self.jobqueue:
                jobs.append(jb)
        return jobs

    @property
    def logger(self):
        if self.quiet:
            logging.disable()
        return logging.getLogger(__package__)

    def clean_jobs(self):
        if self.mode == "sge":
            try:
                self.deletejob(name=self.name)
            except:
                self.qdel(name=self.name)
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
        self.safe_exit()
        if threading.current_thread().name == 'MainThread':
            raise QsubError(self.err_msg)
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

    def clean_resource(self):
        h = ParseSingal(obj=self)
        h.start()

    def safe_exit(self):
        with self.lock:
            self.clean_jobs()
            if self.err_msg:
                self.logger.error(self.err_msg)
            self.sumstatus()

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


def main():
    parser = runsge_parser()
    args = parser.parse_args()
    if args.jobfile.isatty():
        parser.print_help()
        return
    conf = load_config()
    if args.ini:
        conf.update_config(args.ini)
    if args.config:
        print_config(conf)
        return
    if args.local:
        args.mode = "local"
    if args.jobfile is sys.stdin:
        jobfile = args.jobfile.readlines()
        args.jobfile.close()
        args.jobfile = jobfile
    else:
        args.jobfile.close()
        args.jobfile = args.jobfile.name
        if args.logdir is None:
            args.logdir = parser.prog + "_" + \
                basename(args.jobfile) + "_log_dir"
    if args.logdir:
        args.logdir = join(args.workdir, args.logdir)
    else:
        args.logdir = join(args.workdir, parser.prog + "_log_dir")
    if not isdir(args.workdir):
        os.makedirs(args.workdir)
    if args.dot:
        args.quiet = True
    conf.update_dict(**args.__dict__)
    logger = getlog(logfile=args.log,
                    level="debug" if args.debug else "info", name=__package__)
    runsge = RunJob(config=conf)
    try:
        runsge.run()
    except (JobFailedError, QsubError) as e:
        if args.quiet:
            raise e
        sys.exit(10)
    except Exception as e:
        raise e


if __name__ == "__main__":
    sys.exit(main())
