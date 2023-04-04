#!/usr/bin/env python
# coding:utf-8

from . import dag
from .utils import *
from .job import Jobfile
from .sge_run import RunSge
from .config import load_config


class CleanUpSingal(Thread):

    def __init__(self, obj=None):
        super(CleanUpSingal, self).__init__()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.obj = obj
        self.mode = obj.mode
        self.daemon = True
        self.killed = False

    def run(self):
        time.sleep(1)

    def clean(self):
        if self.mode == "sge":
            self.obj.qdel(name=self.obj.name)
            for jb in self.obj.jobqueue.queue:
                jb.remove_all_stat_files()
                self.obj.log_kill(jb)
        for j, p in self.obj.localprocess.items():
            if p.poll() is None:
                terminate_process(p.pid)
            p.wait()
            self.obj.log_kill(self.obj.totaljobdict[j])

    def signal_handler(self, signum, frame):
        if not self.killed:
            self.clean()
            self.obj.sumstatus()
            self.killed = True
        sys.exit(signum)


class qsub(RunSge):

    def __init__(self, config=None):

        self.conf = config
        self.jobfile = config.jobfile
        self.queue = config.queue
        self.maxjob = config.num
        self.strict = config.strict or False
        self.workdir = config.workdir or os.getcwd()
        self.jf = jf = Jobfile(self.jobfile, mode=config.mode or "sge")
        self.mode = jf.mode
        self.name = os.getpid()
        self.jobs = jf.jobs(
            config.injname, config.startline or 1, config.endline)
        self.logdir = jf.logdir
        self.jobnames = [j.name for j in self.jobs]
        self.totaljobdict = {jf.name: jf for jf in jf.totaljobs}
        self.orders = jf.orders()
        self.is_run = False
        self.localprocess = {}
        self.cloudjob = {}
        self.jobsgraph = dag.DAG()
        self.has_success = []
        self.create_graph()
        self.logger.info("Total jobs to submit: %s" %
                         ", ".join([j.name for j in self.jobs]))
        self.logger.info("All logs can be found in %s directory", self.logdir)
        self.check_already_success()
        self.maxjob = self.maxjob or len(self.jobs)
        self.jobqueue = JobQueue(maxsize=min(max(self.maxjob, 1), 1000))
        self.ncall, self.period = config.rate or 3, 1
        self.sge_jobid = {}

    def create_graph(self):
        for k, v in self.orders.items():
            self.jobsgraph.add_node_if_not_exists(k)
            for i in v:
                self.jobsgraph.add_node_if_not_exists(i)
                self.jobsgraph.add_edge(i, k)
        for jn in self.jobsgraph.all_nodes.copy():
            if jn not in self.jobnames:
                self.jobsgraph.delete_node(jn)
        if len(self.jf.alljobnames) != len(set(self.jf.alljobnames)):
            names = [i for i, j in Counter(
                self.jf.alljobnames).items() if j > 1]
            self.throw("duplicate job name: %s" % " ".join(names))
        if self.jobsgraph.size() == 0:
            for jb in self.jobs:
                self.jobsgraph.add_node_if_not_exists(jb.name)

    def clean_resource(self):
        h = CleanUpSingal(obj=self)
        h.start()

    def qdel(self, name="", jobname=""):
        if name:
            call_cmd(['qdel', "*_%d" % os.getpid()])
            self.sge_jobid.clear()
        if jobname:
            jobid = self.sge_jobid.get(jobname, jobname)
            call_cmd(["qdel", jobid])
            if jobname in self.sge_jobid:
                self.sge_jobid.pop(jobname)


def main():
    args = runjobArgparser()
    conf = load_config()
    if args.local:
        args.mode = "local"
    conf.update_dict(**args.__dict__)
    logger = Mylog(logfile=args.log, level=args.debug and "debug" or "info")
    qjobs = qsub(config=conf)
    qjobs.run(times=args.resub, resubivs=args.resubivs)


if __name__ == "__main__":
    sys.exit(main())
