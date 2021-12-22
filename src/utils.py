import os
import sys
import logging

from subprocess import call, PIPE
from collections import Counter


def Mylog(logfile=None, level="info", name=None):
    logger = logging.getLogger(name)
    if level.lower() == "info":
        logger.setLevel(logging.INFO)
        f = logging.Formatter(
            '[%(levelname)s %(asctime)s] %(message)s')
    elif level.lower() == "debug":
        logger.setLevel(logging.DEBUG)
        f = logging.Formatter(
            '[%(levelname)s %(threadName)s %(asctime)s %(funcName)s(%(lineno)d)] %(message)s')
    if logfile is None:
        h = logging.StreamHandler(sys.stdout)  # default: sys.stderr
    else:
        h = logging.FileHandler(logfile, mode='w')
    h.setFormatter(f)
    logger.addHandler(h)
    return logger


def cleanAll(clear=False, qjobs=None):
    if qjobs is None:
        return
    stillrunjob = qjobs.jobqueue.queue
    if clear:
        pid = os.getpid()
        gid = os.getpgid(pid)
        for jn in stillrunjob:
            if jn.status in ["error", "success"]:
                continue
            jn.status = "killed"
            qjobs.logger.info("job %s status killed", jn.name)
        sumJobs(qjobs)
        call('qdel "*_%d"' % os.getpid(),
             shell=True, stderr=PIPE, stdout=PIPE)
    else:
        for jn in stillrunjob:
            if jn.status in ["error", "success"]:
                continue
            jn.status += "-but-exit"
            qjobs.logger.info("job %s status %s", jn.name, jn.status)
        sumJobs(qjobs)


def sumJobs(qjobs):
    run_jobs = qjobs.jobs
    has_success_jobs = qjobs.has_success
    error_jobs = [j for j in run_jobs if j.status == "error"]
    success_jobs = [j for j in run_jobs if j.status == 'success']

    logger = logging.getLogger()
    status = "All tesks(total(%d), actual(%d), actual_success(%d), actual_error(%d)) in file (%s) finished" % (len(
        run_jobs) + len(has_success_jobs), len(run_jobs), len(success_jobs), len(error_jobs), os.path.abspath(qjobs.jfile))
    if len(error_jobs) == len(run_jobs):
        status += " successfully."
    else:
        status += ", but there are Unsuccessful tesks."
    logger.info(status)

    qjobs.writestates(os.path.join(qjobs.logdir, "job.status.txt"))
    logger.info(str(dict(Counter([j.status for j in run_jobs]))))
