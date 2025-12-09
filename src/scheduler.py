import threading

from pytz import timezone

try:
    from apscheduler.jobstores.memory import MemoryJobStore
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
except:
    pass

from .utils import SingletonType


class ScheduleJob(metaclass=SingletonType):

    def __init__(self, store=None):
        jobstores = store or MemoryJobStore()
        executors = {
            "default": ThreadPoolExecutor(10),
            # "processpool": ProcessPoolExecutor(4)
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 30,
            'replace_existing': True
        }
        self.scheduler = BackgroundScheduler(
            jobstores={"default": jobstores},
            executors=executors,
            job_defaults=job_defaults,
            timezone=timezone('Asia/Shanghai')
        )

    def add_job(self, func, trigger="interval", args=None, kwargs=None, job_id=None, name=None, seconds=10, **trigger_args):
        # func can be wrapped with try ... exception ...
        jid = job_id or func.__name__
        jname = f"task_{jid}"
        self.scheduler.add_job(
            func,
            trigger=trigger,  # date,interval,cron
            args=args,  # list|tuple of func args
            kwargs=kwargs,  # dict of func kwargs
            id=job_id or jid,
            name=name or jname,
            seconds=seconds,
            **trigger_args
        )

    @property
    def jobs(self):
        return self.scheduler.get_jobs()

    def get_job(self, job_id):
        return self.scheduler.get_job(job_id)

    def start(self):
        self.scheduler.start()

    def shutdown(self, wait=False):
        self.scheduler.shutdown(wait=wait)

    def stop(self, job_id=None, stop_all=False):
        if job_id and self.get_job(job_id):
            self.scheduler.pause_job(job_id)
        elif stop_all and self.is_running:
            self.scheduler.pause()

    def remove(self, job_id=None, remove_all=False):
        if job_id and self.get_job(job_id):
            self.stop(job_id)
            self.scheduler.remove_job(job_id)
        elif remove_all:
            self.stop(stop_all=True)
            self.scheduler.remove_all_jobs()

    def clear(self):
        self.remove(remove_all=True)

    def resume(self, job_id=None, resume_all=False):
        if job_id and self.get_job(job_id):
            self.scheduler.resume_job(job_id)
        elif resume_all:
            self.scheduler.resume()

    def modify_job(self, job_id=None, **changes):
        self.scheduler.modify_job(job_id=job_id, **changes)

    def print_jobs(self):
        self.scheduler.print_jobs()

    @property
    def is_running(self):
        return self.scheduler.running

    @property
    def state(self):
        '''
        STATE_STOPPED = 0: stopped state
        STATE_RUNNING = 1: running state, started and processing jobs
        STATE_PAUSED = 2: paused state, started but not processing jobs
        '''
        return self.scheduler.state


def get_scheduler(store=None):
    return ScheduleJob(store=store)
