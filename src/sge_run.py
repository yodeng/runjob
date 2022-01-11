#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
import signal
import getpass
import logging
import argparse
import threading


from . import dag
from .job import *
from .config import load_config, print_config
from .qsub import myQueue, QsubError
from .sge import ParseSingal
from .version import __version__
from .utils import *
from .cluster import *

from datetime import datetime
from threading import Thread
from subprocess import Popen, call, PIPE
from collections import Counter


class RunSge(object):

    def __init__(self, sgefile, queue, cpu, mem, name, start, end, logdir, workdir, maxjob, strict=False, mode=None, config=None):
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

        self.jobsgraph = dag.DAG()
        pre_dep = []
        dep = []
        for jb in self.jobs[:]:
            if jb.rawstring == "wait" and len(dep):
                self.jobs.remove(jb)
                pre_dep = dep
                dep = []
            else:
                if jb.rawstring == "wait":  # dup "wait" line
                    self.jobs.remove(jb)
                    continue
                self.jobsgraph.add_node_if_not_exists(jb.jobname)
                dep.append(jb)
                for i in pre_dep:
                    self.jobsgraph.add_node_if_not_exists(i.jobname)
                    self.jobsgraph.add_edge(i.jobname, jb.jobname)

        if self.conf.get("args", "call_back"):
            cmd = self.conf.get("args", "call_back")
            name = "call_back"
            call_back_job = ShellJob(self.sgefile, linenum=-1, cmd=cmd)
            call_back_job.forceToLocal(jobname=name, removelog=True)
            self.jobsgraph.add_node_if_not_exists(call_back_job.jobname)
            self.jobs.append(call_back_job)
            self.totaljobdict["call_back"] = call_back_job
            for i in self.jobsgraph.all_nodes:
                if i == name:
                    continue
                self.jobsgraph.add_edge(i, name)
        if self.conf.get("args", "init"):
            cmd = self.conf.get("args", "init")
            name = "init"
            init_job = ShellJob(self.sgefile, linenum=-1, cmd=cmd)
            init_job.forceToLocal(jobname=name, removelog=True)
            self.jobsgraph.add_node_if_not_exists(init_job.jobname)
            self.jobs.append(init_job)
            self.totaljobdict[name] = init_job
            for i in self.jobsgraph.all_nodes:
                if i == name:
                    continue
                self.jobsgraph.add_edge(name, i)
        self.logger.info("Total jobs to submit: %s" %
                         ", ".join([j.name for j in self.jobs]))
        self.logger.info("All logs can be found in %s directory", self.logdir)

        self.has_success = set()
        for job in self.jobs[:]:
            lf = job.logfile
            job.subtimes = 0
            if os.path.isfile(lf):
                js = self.jobstatus(job)
                if js != "success":
                    os.remove(lf)
                    job.status = "wait"
                else:
                    self.jobsgraph.delete_node_if_exists(job.jobname)
                    self.has_success.add(job.jobname)
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
                try:
                    with os.popen('tail -n 1 %s' % logfile) as fi:
                        sta = fi.read().split()[-1]
                except IndexError:
                    if status not in ["submit", "resubmit"]:
                        status = "run"
                        sta = status
                if sta == "SUCCESS":
                    status = "success"
                elif sta == "ERROR":
                    status = "error"
                elif sta == "Exiting.":
                    status = "exit"
                else:
                    with os.popen("sed -n '3p' %s" % logfile) as fi:
                        if "RUNNING..." in fi.read():
                            status = "run"
        if status != job.status and self.is_run:
            self.logger.info("job %s status %s", jobname, status)
            job.status = status
            if job.host == "batchcompute":
                with open(logfile, "a") as fo:
                    fo.write("[%s] %s\n" % (
                        datetime.today().strftime("%F %X"), job.status.upper()))
        return status

    def jobcheck(self):
        if self.sgefile.mode == "batchcompute":
            rate_limiter = RateLimiter(max_calls=3, period=3)
        else:
            rate_limiter = RateLimiter(max_calls=3, period=2)
        while True:
            with rate_limiter:
                for jb in self.jobqueue.queue:
                    with rate_limiter:
                        try:
                            js = self.jobstatus(jb)
                        except:
                            continue
                        if js == "success":
                            if jb.jobname in self.localprocess:
                                self.localprocess[jb.jobname].wait()
                            self.jobqueue.get(jb)
                            self.jobsgraph.delete_node_if_exists(jb.jobname)
                        elif js == "error":
                            if jb.jobname in self.localprocess:
                                self.localprocess[jb.jobname].wait()
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
                            if self.strict:
                                self.throw("Error when submit")

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

            if job.host is not None and job.host == "localhost":
                cmd = "echo 'Your job (\"%s\") has been submitted in localhost' && " % job.name + job.cmd
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                p = Popen(cmd, shell=True, stdout=logcmd, stderr=logcmd)
                self.localprocess[job.name] = p
            elif job.host == "sge":
                jobcpu = job.cpu if job.cpu else self.cpu
                jobmem = job.mem if job.mem else self.mem
                cmd = 'echo "%s" | qsub -q %s -wd %s -N %s -o %s -j y -l vf=%dg,p=%d' % (
                    job.cmd, " -q ".join(self.queue), self.sgefile.workdir, job.jobname, logfile, jobmem, jobcpu)
                if job.subtimes > 0:
                    cmd = cmd.replace("RUNNING", "RUNNING (re-submit)")
                    time.sleep(self.resubivs)
                call(cmd, shell=True, stdout=logcmd, stderr=logcmd)
            elif job.host == "batchcompute":
                jobcpu = job.cpu if job.cpu else self.cpu
                jobmem = job.mem if job.mem else self.mem
                c = Cluster(config=self.conf)
                c.AddClusterMount()
                task = Task(c)
                task.AddOneTask(
                    job=job, outdir=self.conf.get("args", "outdir"))
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
            for j in subjobs:
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
                call('qdel "%s*"' % self.sgefile.name,
                     shell=True, stderr=PIPE, stdout=PIPE)
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
                        self.logger.info("Delete job %s success", jobid)
                        self.jobqueue.get(jb)
                    else:
                        self.logger.info(
                            "Delete job error, you have no assess with job %s", jobid)

    def writestates(self, outstat):
        summary = {j.name: j.status for j in self.jobs}
        with open(outstat, "w") as fo:
            fo.write(str(dict(Counter(summary.values()))) + "\n\n")
            sumout = {}
            for k, v in summary.items():
                sumout.setdefault(v, []).append(k)
            for k, v in sorted(sumout.items()):
                fo.write(
                    k + " : " + ", ".join(sorted(v, key=lambda x: (len(x), x))) + "\n")


def parserArg():
    pid = os.getpid()
    parser = argparse.ArgumentParser(
        description="For multi-run your shell scripts localhost or qsub.")
    parser.add_argument("-q", "--queue", type=str, help="the queue your job running, default: all.q",
                        default=["all.q", ], nargs="*", metavar="<queue>")
    parser.add_argument("-m", "--memory", type=int,
                        help="the memory used per command (GB), default: 1", default=1, metavar="<int>")
    parser.add_argument("-c", "--cpu", type=int,
                        help="the cpu numbers you job used, default: 1", default=1, metavar="<int>")
    parser.add_argument("-wd", "--workdir", type=str, help="work dir, default: %s" %
                        os.path.abspath(os.getcwd()), default=os.path.abspath(os.getcwd()), metavar="<workdir>")
    parser.add_argument("-N", "--jobname", type=str,
                        help="job name", metavar="<jobname>")
    parser.add_argument("-lg", "--logdir", type=str,
                        help='the output log dir, default: "runjob_*_log_dir"', metavar="<logdir>")
    parser.add_argument("-o", "--outdir", type=str,
                        help='the oss output directory if your mode is "batchcompute", all output file will be mapping to you OSS://BUCKET-NAME. if not set, any output will be reserved', metavar="<dir>")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time. default: all in your job file", metavar="<int>")
    parser.add_argument("-s", "--startline", type=int,
                        help="which line number(0-base) be used for the first job tesk. default: 0", metavar="<int>", default=0)
    parser.add_argument("-e", "--endline", type=int,
                        help="which line number (include) be used for the last job tesk. default: all in your job file", metavar="<int>")
    parser.add_argument('-d', '--debug', action='store_true',
                        help='log debug info', default=False)
    parser.add_argument("-l", "--log", type=str,
                        help='append log info to file, sys.stdout by default', metavar="<file>")
    parser.add_argument('-r', '--resub', help="rebsub you job when error, 0 or minus means do not re-submit, 0 by default",
                        type=int, default=0, metavar="<int>")
    parser.add_argument('--init', help="initial command before all task if set, will be running in localhost",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--call-back', help="callback command if set, will be running in localhost",
                        type=str,  metavar="<cmd>")
    parser.add_argument('--mode', type=str, default="sge", choices=[
                        "sge", "local", "localhost", "batchcompute"], help="the mode to submit your jobs, 'sge' by default")
    parser.add_argument('--access-key-id', type=str,
                        help="AccessKeyID while access oss", metavar="<str>")
    parser.add_argument('--access-key-secret', type=str,
                        help="AccessKeySecret while access oss", metavar="<str>")
    parser.add_argument('--regin', type=str, default="BEIJING", choices=['BEIJING', 'HANGZHOU', 'HUHEHAOTE', 'SHANGHAI',
                        'ZHANGJIAKOU', 'CHENGDU', 'HONGKONG', 'QINGDAO', 'SHENZHEN'], help="batch compute regin, BEIJING by default")
    parser.add_argument('-ivs', '--resubivs', help="rebsub interval seconds, 2 by default",
                        type=int, default=2, metavar="<int>")
    parser.add_argument('-ini', '--ini',
                        help="input configfile for configurations search.", metavar="<configfile>")
    parser.add_argument("-config", '--config',   action='store_true',
                        help="show configurations and exit.",  default=False)
    # parser.add_argument("--local", default=False, action="store_true",
    # help="submit your jobs in localhost instead of sge, if no sge installed, always localhost.")
    parser.add_argument("--strict", action="store_true", default=False,
                        help="use strict to run. Means if any errors occur, clean all jobs and exit programe. off by default")
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
    parser.add_argument("-i", "--jobfile", type=str,
                        help="the input jobfile", metavar="<jobfile>")
    progargs = parser.parse_args()
    return progargs


def main():
    args = parserArg()
    conf = load_config()
    if args.ini:
        conf.update_config(args.ini)
    conf.update_dict(**args.__dict__)
    if args.config:
        print_config(conf)
        sys.exit()
    if args.jobfile is None:
        raise IOError("-i/--jobfile must be required")
    name = args.jobname
    if name is None:
        name = os.path.basename(args.jobfile) + "_" + str(os.getpid())
        if name[0].isdigit():
            name = "job_" + name
    args.jobname = name
    if args.logdir is None:
        args.logdir = os.path.join(os.path.abspath(os.path.dirname(
            args.jobfile)), "runjob_"+os.path.basename(args.jobfile) + "_log_dir")
    conf.update_dict(**args.__dict__)
    h = ParseSingal(name=args.jobname, mode=args.mode, conf=conf)
    h.start()
    logger = Mylog(logfile=args.log,
                   level="debug" if args.debug else "info", name=__name__)
    runsge = RunSge(args.jobfile, args.queue, args.cpu, args.memory, args.jobname,
                    args.startline, args.endline, args.logdir, args.workdir, args.num, args.strict, mode=args.mode, config=conf)
    runsge.run(times=args.resub, resubivs=args.resubivs)
    success = sumJobs(runsge)
    if success:
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
