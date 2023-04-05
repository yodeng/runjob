#!/usr/bin/env python
# coding:utf-8

import os
import sys
import pdb
import time
import signal
import getpass
import logging
import argparse

from . import dag
from .job import *
from .utils import *
from .cluster import *
from .sge import ParseSingal
from ._version import __version__
from .config import load_config, print_config


class RunSge(object):

    def __init__(self, config=None):
        '''
        all attribute of config:

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
            @logdir <dir>: defalut: "%s/runjob_*_log_dir" % os.getcwd()
            @workdir <dir>: default: os.getcwd()
            @rate <int>: default: 3
        '''
        self.conf = config
        self.jobfile = config.jobfile
        if not self.jobfile:
            raise self.throw("Empty jobs input")
        self.queue = config.queue
        self.maxjob = config.num
        self.cpu = config.cpu or 1
        self.mem = config.memory or 1
        self.groups = config.groups or 1
        self.strict = config.strict or False
        self.workdir = config.workdir or os.getcwd()
        self.sgefile = ShellFile(self.jobfile, mode=config.mode or "sge", name=config.jobname,
                                 logdir=config.logdir, workdir=self.workdir)
        self.logdir = self.sgefile.logdir
        self.jfile = self.sgefile._path
        self.jobs = self.sgefile.jobshells(
            start=config.startline or 1, end=config.endline)
        self.mode = self.sgefile.mode
        self.name = self.sgefile.name
        self.totaljobdict = {j.jobname: j for j in self.jobs}
        self.is_run = False
        self.localprocess = {}
        self.cloudjob = {}
        self.jobsgraph = dag.DAG()
        self.has_success = []
        self.depency_jobs()
        self.group_jobs()
        self.init_callback()
        self.logger.info("Total jobs to submit: %s" %
                         ", ".join([j.name for j in self.jobs]))
        self.logger.info("All logs can be found in %s directory", self.logdir)
        self.check_already_success()
        self.maxjob = self.maxjob or len(self.jobs)
        self.jobqueue = JobQueue(maxsize=min(max(self.maxjob, 1), 1000))
        self.conf.jobqueue = self.jobqueue
        self.conf.logger = self.logger
        self.conf.cloudjob = self.cloudjob
        self.rate = Fraction(config.rate or 3).limit_denominator()
        self.sge_jobid = {}

    def depency_jobs(self):
        cur_jobs, dep_jobs = [], []
        for j in self.jobs[:]:
            if j.rawstring == "wait":
                if cur_jobs:
                    dep_jobs = cur_jobs[:]
                    cur_jobs = []
            else:
                self.jobsgraph.add_node_if_not_exists(j.jobname)
                if dep_jobs:
                    for dep_j in dep_jobs:
                        self.jobsgraph.add_edge(dep_j.jobname, j.jobname)
                cur_jobs.append(j)

    def group_jobs(self):
        jobs_groups = []
        jgs = []
        for j in self.jobs[:]:
            if j.rawstring == "wait":
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
                        self._make_groups(wait_groups[n:n+jb.groups])
                        i = jb.groups+n
                    else:
                        self.throw('groups conflict in "%s" line number %d: "%s"' % (self.jfile,
                                                                                     jb.linenum, jb.cmd0))
                elif n >= i and (n-i) % self.groups == 0:
                    gs = []
                    for j in wait_groups[n:n+self.groups]:
                        if j.groups:
                            break
                        gs.append(j)
                    self._make_groups(gs)

    def _make_groups(self, jobs=None):
        if len(jobs) > 1:
            j_header = jobs[0]
            for j in jobs[1:]:
                j_header.rawstring += "\n" + j.rawstring
                if j in self.jobs:
                    self.jobs.remove(j)
                self.jobsgraph.delete_node_if_exists(j.jobname)
            j_header.raw2cmd()
            self.totaljobdict[j_header.jobname] = j_header

    def check_already_success(self):
        for job in self.jobs[:]:
            lf = job.logfile
            job.subtimes = 0
            job.remove_all_stat_files()
            if os.path.isfile(lf):
                js = self.jobstatus(job)
                if js != "success":
                    os.remove(lf)
                    job.status = "wait"
                elif hasattr(job, "logcmd") and job.logcmd.strip() != job.rawstring.strip():
                    self.logger.info(
                        "job %s status already success, but raw command changed, will re-running", job.jobname)
                    os.remove(lf)
                    job.status = "wait"
                else:
                    if self.conf.force:
                        self.logger.info(
                            "job %s status already success, but force to re-running", job.jobname)
                        os.remove(lf)
                        job.status = "wait"
                    else:
                        self.jobsgraph.delete_node_if_exists(job.jobname)
                        self.has_success.append(job.jobname)
                        self.jobs.remove(job)
            else:
                job.status = "wait"

    def init_callback(self):
        for name in ["init", "call_back"]:
            cmd = self.conf.get("args", name)
            if not cmd:
                continue
            job = ShellJob(self.sgefile, linenum=-1, cmd=cmd)
            job.forceToLocal(jobname=name, removelog=False)
            self.totaljobdict[name] = job
            if name == "init":
                self.jobs.insert(0, job)
                f = self.jobsgraph.ind_nodes()
                self.jobsgraph.add_node_if_not_exists(job.jobname)
                for j in f:
                    self.jobsgraph.add_edge(name, j)
            else:
                self.jobs.append(job)
                f = [i for i, j in self.jobsgraph.graph.items() if not len(j)]
                self.jobsgraph.add_node_if_not_exists(job.jobname)
                for j in f:
                    self.jobsgraph.add_edge(j, name)

    def log_status(self, job):
        name = job.jobname
        if name in self.cloudjob:
            name = self.cloudjob[name]
        if job.is_fail:
            level = "error"
        elif job.status == "resubmit":
            level = "warn"
        else:
            level = "info"
        getattr(self.logger, level)("job %s status %s", name, job.status)

    def log_kill(self, jb):
        '''
        may be status delay
        '''
        if not jb.is_killed:
            jb.set_kill()
        # if jb.is_killed:
        #    self.log_status(jb)

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
        else:
            if job.host and job.host == "sge" and self.is_run and not os.path.isfile(job.stat_file+".submit"):
                if os.path.isfile(job.stat_file+".success"):
                    status = "success"
                elif os.path.isfile(job.stat_file+".error"):
                    status = "error"
                elif os.path.isfile(job.stat_file+".run"):
                    status = "run"
            else:
                if os.path.isfile(logfile):
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
        self.logger.debug("job %s status %s", jobname, status)
        if status != job.status and self.is_run:
            job.set_status(status)
            self.log_status(job)
            if job.host == "batchcompute":
                with open(logfile, "a") as fo:
                    fo.write("[%s] %s\n" % (
                        datetime.today().strftime("%F %X"), job.status.upper()))
        return status

    def set_rate(self, rate=3):
        if rate:
            self.rate = Fraction(rate).limit_denominator()

    def jobcheck(self):
        if self.mode == "batchcompute":
            self.set_rate(1)
        rate_limiter = RateLimiter(
            max_calls=self.rate.numerator, period=self.rate.denominator)
        while True:
            for jb in self.jobqueue.queue:
                with rate_limiter:
                    try:
                        js = self.jobstatus(jb)
                    except:
                        self.logger.error(
                            "check job status error: %s", jb.name)
                        continue
                    if js == "success":
                        self.deletejob(jb)
                        self.jobqueue.get(jb)
                        self.jobsgraph.delete_node_if_exists(jb.jobname)
                    elif js == "error":
                        self.deletejob(jb)
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
                    elif js == "exit":
                        self.deletejob(jb)
                        self.jobqueue.get(jb)
                        self.jobsgraph.delete_node_if_exists(jb.jobname)
                        if self.strict:
                            self.throw("Error job: %s, exit" % jb.jobname)

    def qdel(self, name="", jobname=""):
        if name:
            call_cmd(['qdel', "%s_%d*" % (name, os.getpid())])
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
                self.log_kill(jb)
        else:
            if jb.jobname in self.localprocess:
                p = self.localprocess.pop(jb.jobname)
                if p.poll() is None:
                    terminate_process(p.pid)
                p.wait()
                self.log_kill(jb)
            if jb.host == "sge":
                self.qdel(jobname=jb.jobname)
                jb.remove_all_stat_files()
                self.log_kill(jb)
            if self.is_run:
                if jb.is_end:
                    jb.remove_all_stat_files()
                elif jb.status == "run":
                    self.remove_stat_file(".success", ".error", ".submit")

    def submit(self, job):
        if not self.is_run or job.do_not_submit:
            return
        logfile = job.logfile
        self.jobqueue.put(job, block=True, timeout=1080000)
        with open(logfile, "a") as logcmd:
            if job.subtimes == 0:
                logcmd.write(job.rawstring+"\n")
                job.set_status("submit")
            elif job.subtimes > 0:
                logcmd.write("\n" + job.rawstring+"\n")
                job.set_status("resubmit")
            self.log_status(job)
            logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
            logcmd.flush()
            if job.host is not None and job.host in ["localhost", "local"]:
                cmd = "echo 'Your job (\"%s\") has been submitted in localhost' && " % job.name + job.cmd
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                if job.workdir != self.workdir:
                    if not os.path.isdir(job.workdir):
                        os.makedirs(job.workdir)
                    os.chdir(job.workdir)
                    p = Popen(cmd, shell=True, stdout=logcmd,
                              stderr=logcmd, env=os.environ)
                    os.chdir(self.workdir)
                else:
                    p = Popen(cmd, shell=True, stdout=logcmd,
                              stderr=logcmd, env=os.environ)
                self.localprocess[job.name] = p
            elif job.host == "sge":
                call_cmd(["touch", job.stat_file + ".submit"])
                jobcpu = job.cpu or self.cpu
                jobmem = job.mem or self.mem
                sge_queue = job.queue or self.queue
                cmd = job.qsub_cmd(jobmem, jobcpu)
                if sge_queue:
                    cmd += " -q " + " -q ".join(sge_queue)
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                sgeid, output = self.sge_qsub(cmd)
                self.sge_jobid[job.jobname] = sgeid
                logcmd.write(output)
            elif job.host == "batchcompute":
                jobcpu = job.cpu if job.cpu else self.cpu
                jobmem = job.mem if job.mem else self.mem
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
            self.logger.debug("%s job submit %s times", job.name, job.subtimes)
            job.subtimes += 1

    def sge_qsub(self, cmd):
        p = Popen(cmd.replace("`", "\`"), stderr=PIPE, stdout=PIPE, shell=True)
        stdout, stderr = p.communicate()
        output = stdout + stderr
        match = QSUB_JOB_ID_DECODER.search(output.decode())
        if match:
            jobid = match.group(1)
        else:
            self.throw(output.decode())
        return jobid, output.decode()

    def run(self, sec=2, times=0, resubivs=2):
        '''
        @sec: submit epoch ivs, default: 2
        @times: resubmit times, default: 0
        @resubivs: resubmit ivs sec, default: 2
        '''
        self.is_run = True
        self.times = max(0, times)
        self.resubivs = max(resubivs, 0)
        for jn in self.has_success:
            self.logger.info("job %s status already success", jn)
        if len(self.jobsgraph.graph) == 0:
            return
        self.clean_resource()
        p = Thread(target=self.jobcheck)
        p.setDaemon(True)
        p.start()
        if self.mode == "batchcompute":
            access_key_id = self.conf.args.access_key_id or self.conf.access_key_id
            access_key_secret = self.conf.args.access_key_secret or self.conf.access_key_secret
            region = REGION.get(self.conf.args.region, CN_BEIJING)
            client = Client(region, access_key_id, access_key_secret)
            quotas = client.get_quotas().AvailableClusterInstanceType
            cfg_path = os.path.join(os.path.dirname(__file__), "ins_type.json")
            with open(cfg_path) as fi:
                self.conf.it_conf = json.load(fi)
            availableTypes = [i for i in quotas if i in self.conf.it_conf]
            self.conf.availableTypes = sorted(availableTypes, key=lambda x: (
                self.conf.it_conf[x]["cpu"], self.conf.it_conf[x]["memory"]))
            self.conf.client = self.client = client
        while True:
            subjobs = self.jobsgraph.ind_nodes()
            if len(subjobs) == 0:
                break
            for j in sorted(subjobs):
                jb = self.totaljobdict[j]
                if jb in self.jobqueue._queue:
                    continue
                self.submit(jb)
            time.sleep(sec)
        if not self.is_success:
            os.kill(os.getpid(), signal.SIGUSR1)

    @property
    def logger(self):
        return logging.getLogger(__name__)

    def clean_jobs(self):
        if self.mode == "sge":
            try:
                self.deletejob(name=self.name)
            except:
                self.qdel(name=self.name)
            for jb in self.jobqueue.queue:
                jb.remove_all_stat_files()
                self.log_kill(jb)
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
        for j, p in self.localprocess.items():
            if p.poll() is None:  # still running
                terminate_process(p.pid)
            p.wait()
            self.log_kill(self.totaljobdict[j])

    def throw(self, msg=""):
        if threading.current_thread().name == 'MainThread':
            self.sumstatus()
            raise QsubError(msg)
        else:
            if self.mode == "sge":
                self.clean_jobs()
                self.logger.error(msg)
            if self.strict:
                # force exit
                os.kill(os.getpid(), signal.SIGTERM)
            else:
                # SystemExit Exception
                os.kill(os.getpid(), signal.SIGUSR1)

    def writestates(self, outstat):
        summary = {j.name: self.totaljobdict[j.name].status for j in self.jobs}
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(summary.values()))) + "\n")
            fo.write("# Detail:\n")
            sumout = {}
            for k, v in summary.items():
                sumout.setdefault(v, []).append(k)
            for k, v in sorted(sumout.items()):
                fo.write(
                    k + " : " + ", ".join(sorted(v, key=lambda x: (len(x), x))) + "\n")

    def clean_resource(self):
        name = self.name
        conf = self.conf
        mode = self.mode
        h = ParseSingal(obj=self, name=name, mode=mode, conf=conf)
        h.start()

    @property
    def is_success(self):
        return all(j.is_success for j in self.jobs)

    def sumstatus(self):
        if not hasattr(self, "jobs") or not len(self.jobs):
            return
        err_jobs = sum(j.is_fail for j in self.jobs)
        suc_jobs = sum(j.is_success for j in self.jobs)
        wt_jobs = sum(j.is_wait for j in self.jobs)
        total_jobs = len(self.jobs) + len(self.has_success)
        sub_jobs = len(self.jobs) - wt_jobs
        sum_info = "All jobs (total: %d, submited: %d, success: %d, error: %d, wait: %d) " % (
            total_jobs, sub_jobs, suc_jobs, err_jobs, wt_jobs)
        if hasattr(self, "sgefile") and not self.sgefile.temp:
            sum_info += "in file '%s' " % os.path.abspath(self.jfile)
        self.writestates(os.path.join(self.logdir, "job.status.txt"))
        job_counter = str(dict(Counter([j.status for j in self.jobs])))
        if self.is_success:
            sum_info += "finished successfully."
            self.logger.info(sum_info)
            self.logger.info(job_counter)
        else:
            sum_info += "finished, but there are unsuccessful job."
            self.logger.error(sum_info)
            self.logger.error(job_counter)


def main():
    parser = runsgeArgparser()
    args = parser.parse_args()
    conf = load_config()
    if args.ini:
        conf.update_config(args.ini)
    if args.config:
        print_config(conf)
        parser.exit()
    if args.jobfile is None:
        parser.error("argument -j/--jobfile is required")
    if args.local:
        args.mode = "local"
    if not os.path.isdir(args.workdir):
        os.makedirs(args.workdir)
    os.chdir(args.workdir)
    if args.logdir is None:
        args.logdir = "runjob_"+os.path.basename(args.jobfile) + "_log_dir"
    args.logdir = os.path.join(args.workdir, args.logdir)
    conf.update_dict(**args.__dict__)
    logger = Mylog(logfile=args.log,
                   level="debug" if args.debug else "info", name=__name__)
    runsge = RunSge(config=conf)
    runsge.run(times=args.resub, resubivs=args.resubivs)


if __name__ == "__main__":
    sys.exit(main())
