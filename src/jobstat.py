#!/usr/bin/env python
# coding:utf-8
'''For summary all jobs
Usage: qs [jobfile|logdir|logfile]
       qcs --help
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
from .stat_bc import *
from .cluster import *
from .job import Jobfile
from ._version import __version__
from .config import load_config, print_config


def main():
    if len(sys.argv) > 2 or "-h" in sys.argv or "--help" in sys.argv:
        sys.exit(__doc__)
    has_qstat = os.getenv("SGE_ROOT")
    username = os.getenv("USER")
    if has_qstat:
        with os.popen("qstat -u \* | grep -P '^\d+\s+'") as fi:
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
            #raise IOError("No such log_dir %s" % logdir)
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


def bcArgs():
    parser = argparse.ArgumentParser(
        description="for query job status in batch compute.")
    parser.add_argument("-t", "--top", type=int, default=10,
                        help="show top number job. (default: 10)", metavar="<int>")
    parser.add_argument("-a", "--all", action="store_true", default=False,
                        help="show all jobs.")
    parser.add_argument("-n", "--name", type=str,
                        help="show jobName contains the specific name.", metavar="<str>")
    parser.add_argument("-u", "--user", type=str, default=getpass.getuser(),
                        help="show jobs with a user name matching. (defualt: %s)" % getpass.getuser(), metavar="<str>")
    parser.add_argument('-r', '--region', type=str, default="beijing", choices=['beijing', 'hangzhou', 'huhehaote', 'shanghai',
                                                                                'zhangjiakou', 'chengdu', 'hongkong', 'qingdao', 'shenzhen'], help="batch compute region. (default: beijing)")
    parser.add_argument("-d", "--delete", type=str, nargs="*",
                        help="delete job with jobId.", metavar="<jobId>")
    parser.add_argument("-j", "--job", type=str,
                        help="description of the job.", metavar="<jobId>")
    parser.add_argument('--access-key-id', type=str,
                        help="AccessKeyID while access oss.", metavar="<str>")
    parser.add_argument('--access-key-secret', type=str,
                        help="AccessKeySecret while access oss.", metavar="<str>")
    parser.add_argument('-ini', '--ini',
                        help="input configfile for configurations search.", metavar="<configfile>")
    parser.add_argument("-config", '--config',   action='store_true',
                        help="show configurations and exit.",  default=False)
    # parser.add_argument("-l", "--logdir", type=str,
    # help="summary job status in you job directory", metavar="<jobfile>")
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
    args = parser.parse_args()
    return args


def batchStat():
    args = bcArgs()
    conf = load_config()
    if args.ini:
        conf.update_config(args.ini)
    conf.update_dict(**args.__dict__)
    if args.config:
        print_config(conf)
        sys.exit()
    region = REGION.get(args.region.upper(), CN_BEIJING)
    access_key_id = conf.rget("args", "access_key_id")
    access_key_secret = conf.rget("args", "access_key_secret")
    if access_key_id is None:
        access_key_id = conf.rget("OSS", "access_key_id")
    if access_key_secret is None:
        access_key_secret = conf.rget("OSS", "access_key_secret")
    if access_key_secret is None or access_key_id is None:
        sys.exit("No access to connect OSS")
    client = Client(region, access_key_id, access_key_secret)
    logger = getlog(name=__package__)
    user = getpass.getuser()
    if args.job:
        try:
            jd = client.get_job_description(args.job)
        except:
            print("Job Description Error %s" % args.job)
            sys.exit(1)
        print(json.dumps(jd.content, indent=4))
        return
    if args.delete:
        for jobid in args.delete:
            try:
                j = client.get_job(jobid)
            except ClientError as e:
                if e.status == 404:
                    logger.info("Invalid JobId %s", jobid)
                    continue
            except:
                continue
            if j.Name.startswith(user):
                if j.State not in ["Stopped", "Failed", "Finished"]:
                    client.stop_job(jobid)
                client.delete_job(jobid)
                logger.info("Delete job %s done", j.Name)
            else:
                logger.info(
                    "Delete job error, you have no assess with job %s", j.Name)
        return
    jobs = list_jobs(client)
    jobarr_owner = filter_list(items2arr(jobs['Items']), {
                               'Name': {'like': args.user}})
    jobarr_owner = sorted(
        jobarr_owner, key=lambda _x: _x["CreationTime"], reverse=True)
    if args.name:
        filter_job = filter_list(jobarr_owner, {'Name': {'like': args.name}})
    else:
        filter_job = jobarr_owner
    if args.top:
        filter_job = filter_job[:args.top]
    if args.all:
        filter_job = jobarr_owner
    out = []
    out.append([style(i, mode="bold") for i in ["User", "JobId", "JobName",
               "CreationTime", "StartTime", "EndTime", "Elapsed", "JobState"]])
    for j in filter_job:
        ct = j["CreationTime"].strftime(
            "%F %X") if j["CreationTime"] is not None else "null"
        st = j["StartTime"].strftime(
            "%F %X") if j["StartTime"] is not None else "null"
        et = j["EndTime"].strftime(
            "%F %X") if j["EndTime"] is not None else "null"
        jid = j["Id"]
        jname = j["Name"].split(user+"_")[-1]
        state = j["State"]
        if j["StartTime"] is not None and j["EndTime"] is not None:
            dt = j["EndTime"] - j["StartTime"]
            m, s = divmod(dt.seconds, 60)
            elapsed = "%dm%ds" % (m, s)
        else:
            elapsed = "null"
        line = [user, jid, jname, ct, st, et, elapsed, get_job_state(state)]
        out.append(line)
    tb = prettytable.PrettyTable()
    tb.field_names = out[0]
    for line in out[1:]:
        tb.add_row(line)
    print(tb)


if __name__ == "__main__":
    batchStat()
