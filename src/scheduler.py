import threading

from .utils import SingletonType

try:
    from apscheduler.jobstores.memory import MemoryJobStore
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
except ImportError:
    pass


class ScheduleJob(metaclass=SingletonType):
    """Singleton wrapper around APScheduler's BackgroundScheduler.

    Provides a simplified API for scheduling interval/cron jobs with sensible
    defaults (coalescing, single instance, 30 s misfire grace).
    """

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
        )

    def add_job(self, func, trigger="interval", args=None, kwargs=None,
                job_id=None, name=None, seconds=10, **trigger_args):
        """Register a new scheduled job.

        Args:
            func: Callable to execute.
            trigger: APScheduler trigger type ("interval", "cron", "date").
            args: Positional arguments for *func*.
            kwargs: Keyword arguments for *func*.
            job_id: Unique job identifier (defaults to *func* name).
            name: Human-readable job name.
            seconds: Interval in seconds (used with ``trigger="interval"``).
            **trigger_args: Additional arguments forwarded to the APScheduler trigger.
        """
        # func can be wrapped with try ... exception ...
        jid = job_id or func.__name__
        jname = f"task_{jid}" if name is None else name
        self.scheduler.add_job(
            func,
            trigger=trigger,  # date, interval, cron
            args=args,        # list|tuple of func args
            kwargs=kwargs,    # dict of func kwargs
            id=job_id or jid,
            name=name or jname,
            seconds=seconds,
            **trigger_args
        )

    @property
    def jobs(self):
        """Return all currently registered jobs."""
        return self.scheduler.get_jobs()

    def get_job(self, job_id):
        """Return a single job by its ID."""
        return self.scheduler.get_job(job_id)

    def start(self):
        """Start the background scheduler."""
        self.scheduler.start()

    def shutdown(self, wait=False):
        """Shut down the scheduler, optionally waiting for running jobs to finish."""
        self.scheduler.shutdown(wait=wait)

    def stop(self, job_id=None, stop_all=False):
        """Pause a single job or all jobs."""
        if job_id and self.get_job(job_id):
            self.scheduler.pause_job(job_id)
        elif stop_all and self.is_running:
            self.scheduler.pause()

    def remove(self, job_id=None, remove_all=False):
        """Remove a single job or all jobs from the scheduler."""
        if job_id and self.get_job(job_id):
            self.stop(job_id)
            self.scheduler.remove_job(job_id)
        elif remove_all:
            self.stop(stop_all=True)
            self.scheduler.remove_all_jobs()

    def clear(self):
        """Remove all scheduled jobs."""
        self.remove(remove_all=True)

    def resume(self, job_id=None, resume_all=False):
        """Resume a paused job or all paused jobs."""
        if job_id and self.get_job(job_id):
            self.scheduler.resume_job(job_id)
        elif resume_all:
            self.scheduler.resume()

    def modify_job(self, job_id=None, **changes):
        """Modify an existing job's attributes."""
        self.scheduler.modify_job(job_id=job_id, **changes)

    def print_jobs(self):
        """Print all scheduled jobs to stdout."""
        self.scheduler.print_jobs()

    @property
    def is_running(self):
        """``True`` when the scheduler is actively running."""
        return self.scheduler.running

    @property
    def state(self):
        """Return the scheduler state code.

        - STATE_STOPPED = 0: stopped state
        - STATE_RUNNING = 1: running state, started and processing jobs
        - STATE_PAUSED = 2: paused state, started but not processing jobs
        """
        return self.scheduler.state


def get_scheduler(store=None):
    """Factory: return the singleton :class:`ScheduleJob` instance."""
    return ScheduleJob(store=store)
