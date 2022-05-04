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
from .version import __version__
from .qsub import myQueue, QsubError
from .config import load_config, print_config

from copy import deepcopy
from threading import Thread
from datetime import datetime
from collections import Counter
from subprocess import Popen, call, PIPE


class RunSge(object):

    def __init__(self, config=None):
        sgefile = config.get("args", "jobfile")
        queue = config.get("args", "queue")
        cpu = config.get("args", "cpu")
        mem = config.get("args", "memory")
        name = config.get("args", "jobname")
        start = config.get("args", "startline")
        end = config.get("args", "endline")
        logdir = config.get("args", "logdir")
        workdir = config.get("args", "workdir")
        maxjob = config.get("args", "num")
        groups = config.get("args", "groups")
        strict = config.get("args", "strict")
        mode = config.get("args", "mode")
        self.ssh = None
        if mode == "sge" and config.get("args", "user"):
            self.ssh = SshRemoteHost(host="localhost", port=22)
            if config.get("args", "rsa_key_file"):
                self.ssh.login(username=config.get("args", "user"),
                               pkeyfile=config.get("args", "rsa_key_file"))
            elif config.get("args", "passwd"):
                self.ssh.login(username=config.get("args", "user"),
                               password=config.get("args", "passwd"))
            else:
                rsa_key_file = os.path.join(
                    os.getenv("HOME"), ".ssh", "id_rsa.%s" % config.get("args", "user"))
                if os.path.isfile(rsa_key_file):
                    self.ssh.login(username=config.get("args", "user"),
                                   pkeyfile=rsa_key_file)
        self.sgefile = ShellFile(sgefile, mode=mode, name=name,
                                 logdir=logdir, workdir=workdir)
        self.jfile = self.sgefile._path
        self.jobs = self.sgefile.jobshells(start=start, end=end)
        self.totaljobdict = {j.jobname: j for j in self.jobs}
        self.queue = queue
        self.mem = mem
        self.cpu = cpu
        self.maxjob = maxjob
        self.logdir = logdir
        self.is_run = False
        self.strict = strict
        self.localprocess = {}
        self.cloudjob = {}
        self.conf = config
        self.groups = groups
        self.jobsgraph = dag.DAG()
        j_groups = []
        tmp = []
        for j in self.jobs[:]:
            if j.rawstring == "wait":
                self.jobs.remove(j)
                if tmp:
                    j_groups.append(tmp)
                    tmp = []
            else:
                tmp.append(j)
        if tmp:
            j_groups.append(tmp)
        j_pre = []
        for j_group in j_groups:
            j_dep = []
            for one_group in [j_group[i:i+self.groups] for i in range(0, len(j_group), self.groups)]:
                j0 = one_group[0]
                for ji in one_group[1:]:
                    j0.rawstring += "\n" + ji.rawstring
                    self.jobs.remove(ji)
                j0.raw2cmd()
                self.totaljobdict[j0.jobname] = j0
                self.jobsgraph.add_node_if_not_exists(j0.jobname)
                if j_pre:
                    for j in j_pre:
                        self.jobsgraph.add_edge(j.jobname, j0.jobname)
                j_dep.append(j0)
            j_pre = j_dep

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

        self.logger.info("Total jobs to submit: %s" %
                         ", ".join([j.name for j in self.jobs]))
        self.logger.info("All logs can be found in %s directory", self.logdir)

        self.has_success = []
        for job in self.jobs[:]:
            lf = job.logfile
            job.subtimes = 0
            if os.path.isfile(lf):
                js = self.jobstatus(job)
                if js != "success":
                    os.remove(lf)
                    job.status = "wait"
                elif hasattr(job, "logcmd") and job.logcmd.strip() != job.rawstring.strip():
                    self.logger.info(
                        "job %s status already success, but raw command changed, will re-runing", job.jobname)
                    os.remove(lf)
                    job.status = "wait"
                else:
                    self.jobsgraph.delete_node_if_exists(job.jobname)
                    self.has_success.append(job.jobname)
                    self.jobs.remove(job)
            else:
                job.status = "wait"

        if self.maxjob is None:
            self.maxjob = len(self.jobs)

        self.jobqueue = myQueue(maxsize=max(self.maxjob, 1))
        self.conf.jobqueue = self.jobqueue
        self.conf.logger = self.logger
        self.conf.cloudjob = self.cloudjob

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
                        with os.popen("qstat -j %s | tail -n 1" % jobname) as fi:
                            info = fi.read()
                            if info.startswith("error") or ("error" in info and "Job is in error" in info):
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
            job.status = status
            if job.host == "batchcompute":
                with open(logfile, "a") as fo:
                    fo.write("[%s] %s\n" % (
                        datetime.today().strftime("%F %X"), job.status.upper()))
        return status

    def jobcheck(self):
        m, p = 3, 1
        if self.sgefile.mode == "batchcompute":
            p = 3
        rate_limiter = RateLimiter(max_calls=m, period=p)
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
                            self.jobqueue.get(jb)
                            if jb.subtimes >= self.times + 1:
                                if self.strict:
                                    self.throw("Error jobs return(submit %d times, error), exist!, %s" % (jb.subtimes, os.path.join(
                                        self.logdir, jb.logfile)))  # if error, exit program
                                self.jobsgraph.delete_node_if_exists(
                                    jb.jobname)
                            else:
                                self.submit(jb)
                        elif js == "exit":
                            self.deletejob(jb)
                            if self.strict:
                                self.throw("Error when submit")

    def deletejob(self, jb=None, name=""):
        if name:
            if self.ssh:
                self.ssh.run("qdel %s*" % name)
            else:
                call(['qdel', "%s*" % name],
                     stderr=PIPE, stdout=PIPE)
        else:
            if jb.jobname in self.localprocess:
                self.localprocess[jb.jobname].wait()
            if jb.host == "sge":
                if jb.remote:
                    self.ssh.run("qdel " + jb.jobname)
                else:
                    call(["qdel", jb.jobname],
                         stderr=PIPE, stdout=PIPE)

    def submit(self, job):
        if not self.is_run or job.status in ["run", "submit", "resubmit", "success"]:
            return

        logfile = job.logfile

        self.jobqueue.put(job, block=True, timeout=1080000)

        with open(logfile, "a") as logcmd:
            if job.subtimes == 0:
                logcmd.write(job.rawstring+"\n")
                job.status = "submit"
            elif job.subtimes > 0:
                logcmd.write("\n" + job.rawstring+"\n")
                job.status = "resubmit"

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
                    p = Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)
                    os.chdir(self.sgefile.workdir)
                else:
                    p = Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)
                self.localprocess[job.name] = p
            elif job.host == "sge":
                jobcpu = job.cpu if job.cpu else self.cpu
                jobmem = job.mem if job.mem else self.mem
                self.queue = job.queue if job.queue else self.queue
                cmd = 'echo "%s" | qsub -q %s -wd %s -N %s -o %s -j y -l vf=%dg,p=%d' % (
                    job.cmd, " -q ".join(self.queue), job.workdir, job.jobname, logfile, jobmem, jobcpu)
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                if not self.ssh:
                    call(cmd.replace("`", "\`"), shell=True,
                         stdout=logcmd, stderr=logcmd)
                else:
                    job.remote = True
                    if oct(os.stat(job.workdir).st_mode)[-3:] != "777":
                        call(['chmod', "-R", "777", job.workdir],
                             stdout=PIPE, stderr=PIPE)
                    os.chmod(logfile, 0o666)
                    stdin, stdout, stderr = self.ssh.run(
                        cmd.replace("`", "\`"))
                    logcmd.write(stdout.read().decode()+stderr.read().decode())
                    logcmd.flush()
            elif job.host == "batchcompute":
                jobcpu = job.cpu if job.cpu else self.cpu
                jobmem = job.mem if job.mem else self.mem
                c = Cluster(config=self.conf)
                c.AddClusterMount()
                task = Task(c)
                task.AddOneTask(
                    job=job, outdir=self.conf.get("args", "out_maping"))
                if job.out_maping:
                    task.modifyTaskOutMapping(mapping=job.out_maping)
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
        if threading.current_thread().__class__.__name__ == '_MainThread':
            raise QsubError(msg)
        else:
            if self.sgefile.mode == "sge":
                self.logger.info(msg)
                self.deletejob(name=self.sgefile.name)
                os._exit(signal.SIGTERM)
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


def main():
    args = runsgeArgparser()
    conf = load_config()
    if args.ini:
        conf.update_config(args.ini)
    conf.update_dict(**args.__dict__)
    if args.config:
        print_config(conf)
        sys.exit()
    if args.jobfile is None:
        raise IOError("-j/--jobfile must be required")
    name = args.jobname
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
    if not sumJobs(runsge):
        os.kill(os.getpid(), signal.SIGTERM)


if __name__ == "__main__":
    sys.exit(main())
