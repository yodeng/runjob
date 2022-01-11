#!/usr/bin/env python
# coding:utf-8
# 投递任务，将任务散播到集群SGE,并等待所有任务执行完成时退出。 杀掉本主进程时，由本进程投递的qsub任务也会被杀掉，不接受`kill -9`信号

import os
import sys
import time
import signal
import getpass
import argparse

from shutil import rmtree
from threading import Thread
from datetime import datetime
from subprocess import call, PIPE

from .utils import *
from .cluster import *


class ParseSingal(Thread):
    def __init__(self, name="", mode="sge", conf=None):
        super(ParseSingal, self).__init__()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.name = name
        self.mode = mode
        self.conf = conf

    def run(self):
        time.sleep(1)

    def signal_handler(self, signum, frame):
        user = getpass.getuser()
        if self.mode == "sge":
            call('qdel "%s*"' % self.name, shell=True, stderr=PIPE, stdout=PIPE)
        elif self.mode == "batchcompute":
            jobs = self.conf.jobqueue.queue
            for jb in jobs:
                jobname = jb.name
                try:
                    jobid = self.conf.cloudjob.get(jobname, "")
                    j = self.conf.client.get_job(jobid)
                except ClientError as e:
                    if e.status == 404:
                        self.conf.logger.info("Invalid JobId %s", jobid)
                        continue
                except:
                    continue
                if j.Name.startswith(user):
                    if j.State not in ["Stopped", "Failed", "Finished"]:
                        self.conf.client.stop_job(jobid)
                    self.conf.client.delete_job(jobid)
                    self.conf.logger.info("Delete job %s done", j.Name)
                else:
                    self.conf.logger.info(
                        "Delete job error, you have no assess with job %s", j.Name)
        sys.exit(signum)


def not_empty(s):
    return s and s.strip() and (not s.strip().startswith("#")) and s.strip().lower() != "wait"


def parserArg():
    pid = os.getpid()
    parser = argparse.ArgumentParser(
        description="For multi-run your shell scripts localhost or qsub, please use `runsge` instead.")
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
    parser.add_argument("-o", "--logdir", type=str,
                        help='the output log dir, default: "qsub.out.*"', metavar="<logdir>")
    parser.add_argument("-n", "--num", type=int,
                        help="the max job number runing at the same time. default: all in your job file", metavar="<int>")
    parser.add_argument("-s", "--startline", type=int,
                        help="which line number be used for the first job tesk. default: 1", metavar="<int>", default=1)
    parser.add_argument("-e", "--endline", type=int,
                        help="which line number (include) be used for the last job tesk. default: all in your job file", metavar="<int>")
    parser.add_argument("-b", "--block", action="store_true", default=False,
                        help="if passed, block when submitted you job, default: off")
    parser.add_argument("jobfile", type=str,
                        help="the input jobfile", metavar="<jobfile>")
    progargs = parser.parse_args()
    if progargs.logdir is None:
        progargs.logdir = os.path.join(os.path.abspath(os.path.dirname(
            progargs.jobfile)), "qsub.out."+os.path.basename(progargs.jobfile))
    if progargs.jobname is None:
        progargs.jobname = os.path.basename(progargs.jobfile) + "_" + str(pid) if not os.path.basename(
            progargs.jobfile)[0].isdigit() else "job_" + os.path.basename(progargs.jobfile) + "_" + str(pid)
    with open(progargs.jobfile) as fi:
        allcmds = fi.readlines()
        if progargs.endline is None:
            progargs.endline = len(allcmds)
        allcmds = allcmds[(progargs.startline-1):progargs.endline]
    allcmds = filter(not_empty, allcmds)
    progargs.alljobs = len(allcmds)
    if progargs.alljobs == 0:
        print("Error: No jobs for submitted!")
        sys.exit(os.EX_USAGE)
    if progargs.num is None:
        progargs.num = progargs.alljobs
    return progargs


def qsubCheck(jobname, num, block, sec=1):
    qs = 0
    global wait
    while True:
        time.sleep(sec)  # check per 1 seconds
        if not q.full():
            continue
        qs = os.popen('qstat -xml | grep %s | wc -l' % jobname).read().strip()
        qs = int(qs)
        if block or wait:
            if qs == 0:
                while True:
                    if q.empty():
                        wait = False
                        break
                    q.get()
            else:
                continue
        else:
            if qs < num:
                [q.get() for _ in range(num-qs)]
            else:
                continue


def checkAllSuccess(logdir):
    logfile = [os.path.join(logdir, i) for i in os.listdir(logdir)]
    stat = []
    for f in logfile:
        with open(f) as fi:
            ctx = fi.readlines()[-1].strip().split()[-1]
        if ctx == "SUCCESS":
            stat.append(True)
        else:
            stat.append(False)
    if len(stat) != 0 and all(stat):
        return True
    else:
        return False


def main():
    args = parserArg()
    jobfile = args.jobfile
    args.num = max(1, args.num)
    global q, jn, wait
    wait = False
    jn = args.jobname
    q = Queue(maxsize=args.num)
    p = Thread(target=qsubCheck, args=(args.jobname, args.num, args.block))
    p.setDaemon(True)
    p.start()
    h = ParseSingal(name=jn)
    h.start()
    success = []
    with open(jobfile) as fi:
        if os.path.isdir(args.logdir):
            oldlog = [os.path.join(args.logdir, i)
                      for i in os.listdir(args.logdir)]
            for ol in oldlog:
                with open(ol) as oll:
                    ocmd = oll.readline().strip()
                    ostat = oll.readlines()[-1].strip().split()[-1]
                    if ocmd and ostat == "SUCCESS":
                        success.append(ocmd)
                    else:
                        rmtree(args.logdir)
                    continue
        os.makedirs(args.logdir)
        for n, line in enumerate(fi):
            line = line.strip().strip("& ")
            if n+1 < args.startline or n+1 > args.endline:
                continue
            if line.lower() == "wait":  # 若碰到wait, 则自动阻塞，等待前面所有任务执行完毕之后再开始投递
                wait = True
                while True:
                    if q.full():
                        break
                    q.put(n)
                continue
            if line and not line.startswith("#"):
                if line in success:
                    continue
                logfile = os.path.join(args.logdir, os.path.basename(
                    jobfile)+".line") + str(n+1) + ".log"
                # 300 hours  3600*300 = 1080000
                q.put(n, block=True, timeout=1080000)
                qsubline = line if line.endswith("ERROR") else line+RUNSTAT
                qsubline = "echo [`date +'%F %T'`] RUNNING... && " + qsubline
                qsubline = qsubline.replace('"', '\\"')
                cmd = 'qsub -q %s -wd %s -N "%s" -o %s -j y -l vf=%dg,p=%d <<< "%s"' % (" -q ".join(args.queue),
                                                                                        args.workdir, args.jobname+"_" +
                                                                                        str(
                    n+1), logfile, args.memory, args.cpu, qsubline
                )
                logcmd = open(logfile, "w")
                logcmd.write(line+"\n")
                logcmd.write("[%s] " % datetime.today().strftime("%F %X"))
                logcmd.flush()
                call(cmd, shell=True, stdout=logcmd, stderr=logcmd)
                logcmd.close()
    while True:
        time.sleep(2)  # check per 2 seconds
        qs = os.popen('qstat -xml | grep %s | wc -l' %
                      args.jobname).read().strip()
        qs = int(qs)
        if qs == 0:
            # '''
            # 此处可添加操作，所有任务完成之后进行的处理可写在这部分
            # '''
            qsubstat = checkAllSuccess(args.logdir)
            if qsubstat:
                print("[%s] All tesks in file (%s) finished successfully." % (
                    datetime.today().isoformat(), os.path.abspath(args.jobfile)))
                # rmtree(args.logdir)  # 删除logdir
            else:
                print("[%s] All tesks in file (%s) finished, But there are ERROR tesks." % (
                    datetime.today().isoformat(), os.path.abspath(args.jobfile)))
            break
    return


if __name__ == "__main__":
    main()
