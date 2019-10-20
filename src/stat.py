#!/usr/bin/env python
# coding:utf-8
'''For summary all jobs

Usage: runstate [jobfile|logdir]
'''

import os
import sys
import psutil
from collections import defaultdict
from commands import getstatusoutput

from job import Jobfile


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
        print __doc__
        sys.exit(1)
    has_qstat = True if getstatusoutput('command -v qstat')[0] == 0 else False
    username = os.getenv("USER")
    if has_qstat:
        alljobs = os.popen(
            "qstat -u \* | grep -P '^[\d\s]'").readlines()
        jobs = {}
        for j in alljobs:
            j = j.split()
            jobs.setdefault(j[3], defaultdict(int))[j[4]] += 1
        print style("-"*47, mode="bold")
        print style("{0:<20} {1:>8} {2:>8} {3:>8}".format(
            "user", "jobs", "run", "queue"), mode="bold")
        print style("-"*47, mode="bold")
        for u in sorted(jobs.items(), key=lambda x: sum(x[1].values()), reverse=True):
            user = u[0]
            job = sum(u[1].values())
            run = u[1]["r"]
            qw = u[1]["qw"]
            if user == username:
                print style("{0:<20} {1:>8} {2:>8} {3:>8}".format(
                    user, job, run, qw), fore="red", mode="bold")
            else:
                print style("{0:<20} {1:>8} {2:>8} {3:>8}".format(
                    user, job, run, qw))
        print style("-"*47, mode="bold")
        if len(sys.argv) == 2:
            jobfile = sys.argv[1]
            if os.path.isfile(jobfile):

                jf = Jobfile(jobfile)
                jobs = jf.jobs()
                norun = []
                for jn in jf.alljobnames:
                    if not os.path.isfile(os.path.join(jf.logdir, jn + ".log")):
                        norun.append(jn)
                logdir = os.path.abspath(os.path.join(
                    os.path.dirname(jobfile), jf.logdir))
            elif os.path.isdir(jobfile):
                logdir = os.path.abspath(jobfile)
            else:
                raise IOError("No such file or directory %s." % jobfile)
            if not os.path.isdir(logdir):
                raise IOError("No such log_dir %s" % logdir)
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
            print style("-"*47, mode="bold")
            print style("{0:<20} {1:>5}".format("submitted:", submit))
            print style("{0:<20} {1:>5}".format(
                "still runing/queue:", running))
            print style("{0:<20} {1:>5}".format("success:", len(success)))
            print style("{0:<20} {1:>5}".format(
                "error:", len(error)), mode="bold", fore="red")
            print style("-"*47, mode="bold")
            # if len(success) + len(error) + running < submit:
            #    print style("{0} {1}".format("Warning:","some tasks may submite, but not running!" ), mode="bold", fore="red")
            if os.path.isfile(jobfile):
                print "Not submitted jobs: %s" % ", ".join(norun)
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
        print style("-"*70, mode="bold")
        print style("{0:<20} {1:>7} {2:>5} {3:>5} {4:>5} {5:>5} {6:>8} {7:>8}".format(
            "user", "process", "R", "S", "D", "Z", "RES(G)", "VIRT(G)"), mode="bold")
        print style("-"*70, mode="bold")
        for u in sorted(jobs.items(), key=lambda x: sum(x[1].values()), reverse=True):
            user = u[0]
            job = sum(u[1].values())
            res = int(
                round(int(mem[user].get("rss", 0)/1024.0/1024.0/1024.0), 4))
            vms = int(
                round(int(mem[user].get("vms", 0)/1024.0/1024.0/1024.0), 4))
            if user == username:
                print style("{0:<20} {1:>7} {2:>5} {3:>5} {4:>5} {5:>5} {6:>8} {7:>8}".format(user, job, u[1].get("running", 0), u[1].get(
                    "sleeping", 0), u[1].get("disk-sleep", 0), u[1].get("zombie", 0), res, vms), fore="red", mode="bold")
            else:
                print style("{0:<20} {1:>7}".format(user, job)),
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
                print printstr.rstrip()
        print style("-"*70, mode="bold")


if __name__ == "__main__":
    main()
