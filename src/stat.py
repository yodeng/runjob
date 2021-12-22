#!/usr/bin/env python
# coding:utf-8
'''For summary all jobs
Usage: runstate [jobfile|logdir|logfile]
'''

import os
import sys
import psutil
import glob

from collections import defaultdict
from subprocess import check_output

from .job import Jobfile


def style(string, mode='', fore='', back=''):
    STYLE = {
        'fore': {'black': 30, 'red': 31, 'green': 32, 'yellow': 33, 'blue': 34, 'purple': 35, 'cyan': 36, 'white': 37},
        'back': {'black': 40, 'red': 41, 'green': 42, 'yellow': 43, 'blue': 44, 'purple': 45, 'cyan': 46, 'white': 47},
        'mode': {'mormal': 0, 'bold': 1, 'underline': 4, 'blink': 5, 'invert': 7, 'hide': 8},
        'default': {'end': 0},
    }
    mode = '%s' % STYLE["mode"].get(mode, "")
    fore = '%s' % STYLE['fore'].get(fore, "")
    back = '%s' % STYLE['back'].get(back, "")
    style = ';'.join([s for s in [mode, fore, back] if s])
    style = '\033[%sm' % style if style else ''
    end = '\033[%sm' % STYLE['default']['end'] if style else ''
    return '%s%s%s' % (style, string, end)


def main():
    if len(sys.argv) > 2 or "-h" in sys.argv or "--help" in sys.argv:
        print(__doc__)
        sys.exit(1)
    try:
        p = check_output("command -v qstat", shell=True)
        has_qstat = True
    except:
        has_qstat = False
    username = os.getenv("USER")
    if has_qstat:
        with os.popen("qstat -u \* | grep -P '^[\d\s]'") as fi:
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
    else:
        try:
            users = os.popen(
                "awk -F : '{if($3 > 499){print $1}}' /etc/passwd").read().split()
        except:
            users = map(lambda x: os.path.basename(
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
                print(style("{0:<20} {1:>7}".format(user, job))),
                nums = [u[1].get("running", 0), u[1].get("sleeping", 0), u[1].get(
                    "disk-sleep", 0), u[1].get("zombie", 0)]
                printstr = ""
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
        if os.path.isfile(jobfile):
            try:
                jf = Jobfile(jobfile)
                jobs = jf.jobs()
                norun = []
                for jn in jf.alljobnames:
                    if not os.path.isfile(os.path.join(jf.logdir, jn + ".log")):
                        norun.append(jn)
                    else:
                        submit += 1
                logdir = os.path.abspath(os.path.join(
                    os.path.dirname(jobfile), jf.logdir))
            except:
                statfile = jobfile
                stat = {}
                alljobnames = []
                with open(statfile) as fi:
                    for line in fi:
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
                stat2 = {}
                for k, v in stat.items():
                    stat2.setdefault(v, []).append(k)

                submit = sum([len(i) for i in stat2.values()])
                print(style("-"*47, mode="bold"))
                print(style("{0:<20} {1:>5}".format("submitted:", submit)))
                for k, v in stat2.items():
                    num = len(v)
                    if k in ["success", "already success"]:
                        print(style("{0:<20} {1:>5}".format(
                            k+":", num)))
                    elif k == "error":
                        print(style("{0:<20} {1:>5} ".format(
                            "error:", num), mode="bold", fore="red"), ", ".join(v))
                    else:
                        print(style("{0:<20} {1:>5} ".format(
                            k + ":", num)), ", ".join(v))
                wait = set(alljobnames) - set(stat.keys())
                print(style("{0:<20} {1:>5} ".format(
                    "wait:", len(wait))), ", ".join(wait))
                print(style("-"*47, mode="bold"))
                return

        elif os.path.isdir(jobfile):
            logdir = os.path.abspath(jobfile)
        else:
            raise IOError("No such file or directory %s." % jobfile)

        if not os.path.isdir(logdir):
            raise IOError("No such log_dir %s" % logdir)
        if submit == 0:
            submit = len([i for i in os.listdir(
                logdir) if i.endswith(".log")])

        fs = [os.path.join(logdir, i) for i in os.listdir(logdir)]
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
        success = filter(lambda x: x == "SUCCESS", stat)
        error = filter(lambda x: x == "ERROR", stat)
        # running = jobs[username]["r"] + jobs[username]["qw"] if username in jobs else 0
        running = submit - len(success) - len(error)
        print(style("-"*47, mode="bold"))
        print(style("{0:<20} {1:>5}".format("submitted:", submit)))
        print(style("{0:<20} {1:>5}".format(
            "runing/queue/exit:", running)))
        print(style("{0:<20} {1:>5}".format("success:", len(success))))
        print(style("{0:<20} {1:>5}".format(
            "error:", len(error)), mode="bold", fore="red"))
        if os.path.isfile(jobfile):
            if len(norun):
                print(style("{0:<20} {1:>5}".format(
                    "wait:", len(norun))), ", ".join(norun))
        print(style("-"*47, mode="bold"))
        # if len(success) + len(error) + running < submit:
        #    print style("{0} {1}".format("Warning:","some tasks may submite, but not running!" ), mode="bold", fore="red")


if __name__ == "__main__":
    main()
