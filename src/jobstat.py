#!/usr/bin/env python
# coding:utf-8

'''
query local/sge/slurm jobs.

Usage: 
    qs [jobfile|logdir|logfile]
    qslurm
'''

import os
import re
import sys
import pdb
import psutil
import glob
import getpass
import argparse
import prettytable

from subprocess import check_output
from collections import defaultdict

from .utils import *
from .parser import *
from .job import Jobfile
from .context import context
from .config import load_config
from ._version import __version__


def qs():
    if len(sys.argv) > 2 or "-h" in sys.argv or "--help" in sys.argv:
        sys.exit(__doc__)
    username = os.getenv("USER")
    if is_sge_submit():
        with os.popen("qstat -u \* | grep -P '^\s*\d+\s+'") as fi:
            alljobs = fi.readlines()
        jobs = {}
        for j in alljobs:
            j = j.split()
            jobs.setdefault(j[3], defaultdict(int))[j[4]] += 1
        print(style("-"*47, mode="bold"))
        print(style("{0:<20} {1:>8} {2:>8} {3:>8}".format(
            "user", "jobs", "run", "queue"), mode="bold"))
        print(style("-"*47, mode="bold"))
        for u in sorted(jobs.items(), key=lambda x: sum(x[1].values()), reverse=True):
            user = u[0]
            job = sum(u[1].values())
            run = u[1]["r"]
            qw = u[1]["qw"]
            if user == username:
                print(style("{0:<20} {1:>8} {2:>8} {3:>8}".format(
                    user, job, run, qw), fore="red", mode="bold"))
            else:
                print(style("{0:<20} {1:>8} {2:>8} {3:>8}".format(
                    user, job, run, qw)))
        print(style("-"*47, mode="bold"))
    elif is_slurm_host():
        qslurm()
    else:
        try:
            users = os.popen(
                "awk -F : '{if($3 > 499){print $1}}' /etc/passwd").read().split()
        except:
            users = map(lambda x: basename(
                x), os.popen('ls -d /home/*').read().split())
        allpids = psutil.pids()
        jobs = {}
        mem = {}
        for p in allpids:
            try:
                j = psutil.Process(p)
                if j.username() not in users:
                    continue
                RES = j.memory_info().rss
                VIRT = j.memory_info().vms
                jobs.setdefault(j.username(), defaultdict(int))[
                    j.status()] += 1
                mem.setdefault(j.username(), defaultdict(int))["rss"] += RES
                mem.setdefault(j.username(), defaultdict(int))["vms"] += VIRT
            except:
                continue
        print(style("-"*70, mode="bold"))
        print(style("{0:<20} {1:>7} {2:>5} {3:>5} {4:>5} {5:>5} {6:>8} {7:>8}".format(
            "user", "process", "R", "S", "D", "Z", "RES(G)", "VIRT(G)"), mode="bold"))
        print(style("-"*70, mode="bold"))
        for u in sorted(jobs.items(), key=lambda x: sum(x[1].values()), reverse=True):
            user = u[0]
            job = sum(u[1].values())
            res = int(
                round(int(mem[user].get("rss", 0)/1024.0/1024.0/1024.0), 4))
            vms = int(
                round(int(mem[user].get("vms", 0)/1024.0/1024.0/1024.0), 4))
            if user == username:
                print(style("{0:<20} {1:>7} {2:>5} {3:>5} {4:>5} {5:>5} {6:>8} {7:>8}".format(user, job, u[1].get("running", 0), u[1].get(
                    "sleeping", 0), u[1].get("disk-sleep", 0), u[1].get("zombie", 0), res, vms), fore="red", mode="bold"))
            else:
                printstr = style("{0:<20} {1:>7} ".format(user, job))
                nums = [u[1].get("running", 0), u[1].get("sleeping", 0), u[1].get(
                    "disk-sleep", 0), u[1].get("zombie", 0)]
                for i in nums:
                    if i >= 10:
                        printstr += style("{:>5} ".format(i),
                                          fore="red", mode="bold")
                    else:
                        printstr += style("{:>5} ".format(i))
                printstr += style("{:>8} ".format(res)) + \
                    style("{:>8} ".format(vms))
                print(printstr.rstrip())
        print(style("-"*70, mode="bold"))

    if len(sys.argv) == 2:
        jobfile = sys.argv[1]
        submit = 0
        if isfile(jobfile):
            try:
                jf = Jobfile(jobfile)
                jobs = jf.jobs()
                norun = []
                for jn in jf.alljobnames:
                    if not isfile(join(jf.logdir, jn + ".log")):
                        norun.append(jn)
                    else:
                        submit += 1
                logdir = abspath(join(
                    dirname(jobfile), jf.logdir))
            except:
                statfile = jobfile
                stat = {}
                alljobnames = []
                with open(statfile) as fi:
                    for line in fi:
                        line = re.sub('\x1b.*?m', '', line)
                        if line.startswith("[") and "] job " in line:
                            line = line.split()
                            jobname = line[line.index("job")+1]
                            if line[-1] == "success" and line[-2] == "already":
                                stat[jobname] = "already success"
                            else:
                                stat[jobname] = line[-1]
                        elif "All tesks" in line:
                            print(style("Jobfile finish runing...",
                                        mode="bold", fore="red"))
                        elif "Total jobs to submit" in line:
                            alljobnames = line.split(
                                "Total jobs to submit:")[1].split()
                            alljobnames = [j.strip(",") for j in alljobnames]
                            stat.clear()
                stat2 = {}
                for k, v in stat.items():
                    stat2.setdefault(v, []).append(k)

                submit = sum([len(i) for i in stat2.values()])
                print(style("-"*47, mode="bold"))
                print(style("{0:<20} {1:>5}".format("submitted:", submit)))
                for k, v in stat2.items():
                    num = len(v)
                    if k in ["success", "already success"]:
                        out_line = style("{0:<20} {1:>5}".format(k+":", num))
                    elif k == "error":
                        out_line = style("{0:<20} {1:>5}".format(
                            "error:", num), mode="bold", fore="red")
                    else:
                        out_line = style("{0:<20} {1:>5}".format(k + ":", num))
                    if num < 5 or k not in ["success", "already success"]:
                        out_line += "  {}".format(", ".join(v))
                    print(out_line)
                wait = sorted(set(alljobnames) - set(stat.keys()))
                print(style("{0:<20} {1:>5} ".format(
                    "wait:", len(wait)), ", ".join(wait)))
                print(style("-"*47, mode="bold"))
                return

        elif isdir(jobfile):
            logdir = abspath(jobfile)
        else:
            raise IOError("No such file or directory %s." % jobfile)

        if not isdir(logdir):
            return
            # raise IOError("No such log_dir %s" % logdir)
        if submit == 0:
            submit = len([i for i in os.listdir(
                logdir) if i.endswith(".log")])

        fs = [join(logdir, i) for i in os.listdir(logdir)]
        # submit = int(os.popen("awk 'FNR==2' " + " ".join(fs) + " | wc -l ").read().strip())
        submit = 0
        stat = []
        for i in fs:
            if not i.endswith("log"):
                continue
            submit += 1
            s = os.popen("sed -n '$p' " + i).read().strip()
            if s:
                stat.append(s.split()[-1])
            else:
                stat.append("")
        # stat = [os.popen("sed -n '$p' " + i).read().strip().split()[-1] for i in fs]
        # stat = [int(os.popen("grep -i ERROR %s | wc -l"%i).read().strip()) for i in fs]
        success = list(filter(lambda x: x == "SUCCESS", stat))
        error = list(filter(lambda x: x == "ERROR", stat))
        # running = jobs[username]["r"] + jobs[username]["qw"] if username in jobs else 0
        running = submit - len(success) - len(error)
        print(style("-"*47, mode="bold"))
        print(style("{0:<20} {1:>5}".format("submitted:", submit)))
        print(style("{0:<20} {1:>5}".format(
            "runing/queue/exit:", running)))
        print(style("{0:<20} {1:>5}".format("success:", len(success))))
        print(style("{0:<20} {1:>5}".format(
            "error:", len(error)), mode="bold", fore="red"))
        if isfile(jobfile):
            if len(norun):
                print(style("{0:<20} {1:>5}".format(
                    "wait:", len(norun))), ", ".join(norun))
        print(style("-"*47, mode="bold"))
        # if len(success) + len(error) + running < submit:
        #    print style("{0} {1}".format("Warning:","some tasks may submite, but not running!" ), mode="bold", fore="red")


def qslurm():
    if len(sys.argv) > 2 or "-h" in sys.argv or "--help" in sys.argv:
        sys.exit(__doc__)
    username = os.getenv("USER")
    with os.popen("squeue") as fi:
        alljobs = fi.readlines()[1:]
    jobs = {}
    for j in alljobs:
        j = j.split()
        jobs.setdefault(j[3], defaultdict(int))[j[4]] += 1
    print(style("-"*55, mode="bold"))
    print(style("{0:<20} {1:>16} {2:>8} {3:>8}".format(
        "user", "jobs(slurm)", "run", "queue"), mode="bold"))
    print(style("-"*55, mode="bold"))
    for u in sorted(jobs.items(), key=lambda x: sum(x[1].values()), reverse=True):
        '''
        R    正在运行
        PD  资源不足，排队中
        CG   正在完成中
        CA    被人为取消
        CD    运行完成
        F    运行失败
        NF    节点问题导致作业运行失败，
        PR  被抢占
        S   被挂起
        TO  超时被杀
        '''
        user = u[0]
        job = sum(u[1].values())
        run = u[1]["R"]
        qw = u[1]["PD"]
        if user == username:
            print(style("{0:<20} {1:>16} {2:>8} {3:>8}".format(
                user, job, run, qw), fore="red", mode="bold"))
        else:
            print(style("{0:<20} {1:>16} {2:>8} {3:>8}".format(
                user, job, run, qw)))
    print(style("-"*55, mode="bold"))
