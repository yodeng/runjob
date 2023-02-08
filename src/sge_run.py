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
import threading

from . import dag
from .job import *
from .utils import *
from .cluster import *
from .sge import ParseSingal
from ._version import __version__
from .qsub import myQueue, QsubError
from .config import load_config, print_config

from copy import deepcopy
from threading import Thread
from datetime import datetime
from collections import Counter
from subprocess import Popen, call, PIPE, check_output


class RunSge(object):

    def __init__(self, config=None):
        self.conf = config
        self.mode = config.mode
        self.name = config.jobname
        self.logdir = config.logdir
        self.queue = config.queue
        self.mem = config.memory
        self.cpu = config.cpu
        self.maxjob = config.num
        self.groups = config.groups
        self.strict = config.strict
        self.sgefile = ShellFile(config.jobfile, mode=self.mode, name=self.name,
                                 logdir=self.logdir, workdir=config.workdir)
        self.jfile = self.sgefile._path
        self.jobs = self.sgefile.jobshells(
            start=config.startline, end=config.endline)
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
        self.jobqueue = myQueue(maxsize=min(max(self.maxjob, 1), 1000))
        self.conf.jobqueue = self.jobqueue
        self.conf.logger = self.logger
        self.conf.cloudjob = self.cloudjob
        self.ncall, self.period = 3, 1

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
            self.remove_job_stat_files(job)
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
                self.logger.debug("job %s status %s", jobid, status)
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
                            try:
                                info = check_output(
                                    "qstat -j %s" % jobname, shell=True)
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
            self.logger.info("job %s status %s", jobname, status)
            job.set_status(status)
            if job.host == "batchcompute":
                with open(logfile, "a") as fo:
                    fo.write("[%s] %s\n" % (
                        datetime.today().strftime("%F %X"), job.status.upper()))
        return status

    def set_rate(self, ncall=3, period=1):
        self.ncall = ncall
        self.period = period

    def jobcheck(self):
        if self.sgefile.mode == "batchcompute":
            self.set_rate(1, 1)
        rate_limiter = RateLimiter(max_calls=self.ncall, period=self.period)
        while True:
            with rate_limiter:
                for jb in self.jobqueue.queue:
                    with rate_limiter:
                        try:
                            js = self.jobstatus(jb)
                        except:
                            continue
                        if js == "success":
                            self.deletejob(jb)
                            self.jobqueue.get(jb)
                            self.jobsgraph.delete_node_if_exists(jb.jobname)
                        elif js == "error":
                            self.deletejob(jb)
                            if jb.subtimes >= self.times + 1:
                                if self.strict:
                                    self.throw("Error jobs return(submit %d times, error), exist!, %s" % (jb.subtimes, os.path.join(
                                        self.logdir, jb.logfile)))  # if error, exit program
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

    def deletejob(self, jb=None, name=""):
        if name:
            call_cmd(['qdel', "%s*" % name])
        else:
            if jb.jobname in self.localprocess:
                p = self.localprocess.pop(jb.jobname)
                if p.poll() is None:
                    terminate_process(p.pid)
                    jb.set_status("kill")
                p.wait()
            if jb.host == "sge":
                call_cmd(["qdel", jb.jobname])
            if self.is_run:
                if jb.status in ["error", "success", "exit"]:
                    call_cmd(["rm", "-fr", jb.stat_file + ".success", jb.stat_file +
                             ".run", jb.stat_file+".error", jb.stat_file+".submit"])
                elif jb.status == "run":
                    call_cmd(["rm", "-fr", jb.stat_file + ".success",
                             jb.stat_file+".error", jb.stat_file+".submit"])

    def remove_job_stat_files(self, jb):
        call_cmd(["rm", "-fr", jb.stat_file + ".success", jb.stat_file +
                 ".run", jb.stat_file+".error", jb.stat_file+".submit"])

    def submit(self, job):
        if not self.is_run or job.status in ["run", "submit", "resubmit", "success"]:
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
            self.logger.info("job %s status %s", job.name, job.status)
            logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
            logcmd.flush()
            if job.host is not None and job.host in ["localhost", "local"]:
                cmd = "echo 'Your job (\"%s\") has been submitted in localhost' && " % job.name + job.cmd
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                if job.workdir != self.sgefile.workdir:
                    if not os.path.isdir(job.workdir):
                        os.makedirs(job.workdir)
                    os.chdir(job.workdir)
                    p = Popen(cmd, shell=True, stdout=logcmd,
                              stderr=logcmd, env=os.environ)
                    os.chdir(self.sgefile.workdir)
                else:
                    p = Popen(cmd, shell=True, stdout=logcmd,
                              stderr=logcmd, env=os.environ)
                self.localprocess[job.name] = p
            elif job.host == "sge":
                call_cmd(["touch", job.stat_file + ".submit"])
                jobcpu = job.cpu or self.cpu
                jobmem = job.mem or self.mem
                self.queue = job.queue or self.queue
                cmd = 'echo "%s" | qsub -V -wd %s -N %s -o %s -j y -l vf=%dg,p=%d' % (
                    job.cmd, job.workdir, job.jobname, logfile, jobmem, jobcpu)
                if self.queue:
                    cmd += " -q " + " -q ".join(self.queue)
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                call(cmd.replace("`", "\`"), shell=True,
                     stdout=logcmd, stderr=logcmd, env=os.environ)
            elif job.host == "batchcompute":
                jobcpu = job.cpu if job.cpu else self.cpu
                jobmem = job.mem if job.mem else self.mem
                c = Cluster(config=self.conf)
                c.AddClusterMount()
                task = Task(c)
                task.AddOneTask(
                    job=job, outdir=self.conf.get("args", "out_maping"))
                if job.out_maping:
                    task.modifyTaskOutMapping(job=job, mapping=job.out_maping)
                task.Submit()
                info = "Your job (%s) has been submitted in batchcompute (%s) %d times\n" % (
                    task.name, task.id, job.subtimes+1)
                logcmd.write(info)
                self.cloudjob[task.name] = task.id
            self.logger.debug("%s job submit %s times", job.name, job.subtimes)
            job.subtimes += 1

    def run(self, sec=2, times=3, resubivs=2):
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
        if self.sgefile.mode == "batchcompute":
            access_key_id = self.conf.get("args", "access_key_id")
            access_key_secret = self.conf.get("args", "access_key_secret")
            if access_key_id is None:
                access_key_id = self.conf.get("OSS", "access_key_id")
            if access_key_secret is None:
                access_key_secret = self.conf.get("OSS", "access_key_secret")
            region = REGION.get(self.conf.get("args", "region"), CN_BEIJING)
            client = Client(region, access_key_id, access_key_secret)
            quotas = client.get_quotas().AvailableClusterInstanceType
            cfg_path = os.path.join(os.path.dirname(__file__), "ins_type.json")
            with open(cfg_path) as fi:
                self.conf.it_conf = json.load(fi)
            availableTypes = [i for i in quotas if i in self.conf.it_conf]
            self.conf.availableTypes = sorted(availableTypes, key=lambda x: (
                self.conf.it_conf[x]["cpu"], self.conf.it_conf[x]["memory"]))
            self.conf.client = client
        while True:
            subjobs = self.jobsgraph.ind_nodes()
            if len(subjobs) == 0:
                break
            for j in sorted(subjobs):
                jb = self.totaljobdict[j]
                if jb in self.jobqueue.queue:
                    continue
                self.submit(jb)
            time.sleep(sec)

    @property
    def logger(self):
        return logging.getLogger(__name__)

    def throw(self, msg):
        user = getpass.getuser()
        if threading.current_thread().name == 'MainThread':
            self.sumstatus()
            raise QsubError(msg)
        else:
            if self.sgefile.mode == "sge":
                self.logger.info(msg)
                self.deletejob(name=self.sgefile.name)
            elif self.sgefile.mode == "batchcompute":
                for jb in self.jobqueue.queue:
                    jobname = jb.name
                    try:
                        jobid = self.conf.cloudjob.get(jobname, "")
                        j = self.conf.client.get_job(jobid)
                    except ClientError as e:
                        if e.status == 404:
                            self.logger.info("Invalid JobId %s", jobid)
                            continue
                    except:
                        continue
                    if j.Name.startswith(user):
                        if j.State not in ["Stopped", "Failed", "Finished"]:
                            self.conf.client.stop_job(jobid)
                        self.conf.client.delete_job(jobid)
                        self.logger.info("Delete job %s done", j.Name)
                        self.jobqueue.get(jb)
                    else:
                        self.logger.info(
                            "Delete job error, you have no assess with job %s", j.Name)
            else:
                for j, p in self.localprocess.items():
                    if p.poll() is None:
                        terminate_process(p.pid)
                        self.totaljobdict[j].set_status("kill")
                    p.wait()
            self.sumstatus(verbose=True)
            os._exit(signal.SIGTERM)

    def writestates(self, outstat):
        summary = {j.name: self.totaljobdict[j.name].status for j in self.jobs}
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(summary.values()))) + "\n\n")
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

    def sumstatus(self, verbose=False):
        run_jobs = self.jobs
        has_success_jobs = self.has_success
        error_jobs = [j for j in run_jobs if j.status == "error"]
        success_jobs = [j for j in run_jobs if j.status == 'success']
        if not verbose:
            return len(success_jobs) == len(run_jobs) and True or False
        status = "All tesks(total(%d), actual(%d), actual_success(%d), actual_error(%d)) " % (len(
            run_jobs) + len(has_success_jobs), len(run_jobs), len(success_jobs), len(error_jobs))
        if not self.sgefile.temp:
            status += "in file (%s) " % os.path.abspath(self.jfile)
        if len(success_jobs) == len(run_jobs):
            status += "finished successfully."
        else:
            status += "finished, but there are Unsuccessful tesks."
        self.logger.info(status)
        self.writestates(os.path.join(self.logdir, "job.status.txt"))
        self.logger.info(str(dict(Counter([j.status for j in run_jobs]))))


def main():
    parser = runsgeArgparser()
    args = parser.parse_args()
    conf = load_config()
    if args.ini:
        conf.update_config(args.ini)
    conf.update_dict(**args.__dict__)
    if args.config:
        print_config(conf)
        parser.exit()
    if args.jobfile is None:
        parser.error("argument -j/--jobfile is required")
    name = args.jobname
    if args.local:
        args.mode = "local"
    if name is None:
        name = os.path.basename(args.jobfile) + "_" + str(os.getpid())
        if name[0].isdigit():
            name = "job_" + name
    args.jobname = name
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
    h = ParseSingal(obj=runsge, name=args.jobname, mode=args.mode, conf=conf)
    h.start()
    runsge.run(times=args.resub, resubivs=args.resubivs)
    if not runsge.sumstatus():
        os.kill(os.getpid(), signal.SIGTERM)


if __name__ == "__main__":
    sys.exit(main())
