# runjob

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=small)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_small)
[![PyPI version](https://img.shields.io/pypi/v/runjob.svg?logo=pypi&logoColor=FFE873)](https://pypi.python.org/pypi/runjob)
[![Downloads](https://static.pepy.tech/badge/runjob)](https://pepy.tech/project/runjob)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](https://anaconda.org/bioconda/runjob)

## Summary

runjob is a program for managing a group of related jobs running on a compute cluster `localhost`, [Sun Grid Engine](http://star.mit.edu/cluster/docs/0.93.3/guides/sge.html), [slurm](https://slurm.schedmd.com/documentation.html). It provides a convenient method for specifying dependencies between jobs and the resource requirements for each job (e.g. memory, CPU cores). It monitors the status of the jobs so you can tell when the whole group is done. Little CPU or memory resource is used on the login compute node.

## OSCS

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=large)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_large)

## Software Requirements

python >=3.7

## Installation

The development version can be installed with (for recommend)

```
pip install git+https://github.com/yodeng/runjob.git
```

The stable release (maybe not latest) can be installed with

> pypi:

```
pip install runjob -U
```

> conda:

```
conda install -c bioconda runjob
```

## User Guide

All manual can be found [here](https://runjob.readthedocs.io/en/latest/).

## Usage

You can get the quick help like this:

##### runjob/runflow：

```
$ runjob --help
Usage: runjob [-h] [-v] [-j [<jobfile>]] [-n <int>] [-s <int>] [-e <int>] [-w <workdir>] [-d] [-l <file>] [-f] [-M {local,localhost,sge,slurm}] [--config <configfile>] [--dag]
              [--dag-extend] [--strict] [--quiet] [--show-config] [-r <int>] [-R <int>] [--max-check <float>] [--max-submit <float>] [--max-queue-time <float/str>]
              [--max-run-time <float/str>] [--max-wait-time <float/str>] [--max-timeout-retry <int>] [--local | --localhost | --sge | --slurm] [-i [<str> ...]] [-L <logdir>]
              [-q [<queue> ...]] [-c <int>] [-m <int/str>] [--node [<node> ...]] [--round-node]

runjob is a tool for managing parallel tasks from a specific job file running in localhost, sge, slurm.

Options:
  -h, --help            show this help message and exit
  --local               submit your jobs to local, same as '--mode local'.
  --localhost           submit your jobs to localhost, same as '--mode localhost'.
  --sge                 submit your jobs to sge, same as '--mode sge'.
  --slurm               submit your jobs to slurm, same as '--mode slurm'.
  -i, --injname [<str> ...]
                        job names defined to run. (default: all job names of the jobfile)
  -L, --logdir <logdir>
                        the output log dir. (default: join(workdir, "logs"))

Base Arguments:
  -v, --version         show program's version number and exit
  -j, --jobfile [<jobfile>]
                        input jobfile, if empty, stdin is used. (required)
  -n, --num <int>       the max job number running at the same time. (default: all of the jobfile, max 1000)
  -s, --start <int>     which line number (1-base) be used for the first job. (default: 1)
  -e, --end <int>       which line number (include) be used for the last job. (default: last line of the jobfile)
  -w, --workdir <workdir>
                        work directory. (default: current working directory)
  -d, --debug           log debug info.
  -l, --log <file>      append log info to file. (default: stdout)
  -f, --force           force to submit jobs even already succeeded.
  -M, --mode {local,localhost,sge,slurm}
                        the mode to submit your jobs, auto detect. (default: auto)
  --config <configfile>
                        input configfile for configurations search.
  --dag                 do not execute anything and print the directed acyclic graph of jobs in the dot language.
  --dag-extend          do not execute anything and print the extend directed acyclic graph of jobs in the dot language.
  --strict              use strict to run, means if any errors, clean all jobs and exit.
  --quiet               suppress all output and logging.
  --show-config         show configurations and exit.

Rate Arguments:
  -r, --retry <int>     retry N times of the error job, 0 or minus means do not re-submit. (default: 0)
  -R, --retry-sec <int>
                        retry the error job after N seconds. (default: 2)
  --max-check <float>   maximum number of job status checks per second, fractions allowed. (default: 5)
  --max-submit <float>  maximum number of jobs submitted per second, fractions allowed. (default: 20)

Time Arguments:
  --max-queue-time <float/str>
                        maximum time (d/h/m/s) between submit and running per job. (default: no-limiting)
  --max-run-time <float/str>
                        maximum time (d/h/m/s) start from running per job. (default: no-limiting)
  --max-wait-time <float/str>
                        maximum time (d/h/m/s) start from submit per job. (default: no-limiting)
  --max-timeout-retry <int>
                        retry N times for the timeout error job, 0 or minus means do not re-submit. (default: 0)

Resource Arguments:
  -q, --queue [<queue> ...]
                        queue/partition for running, multi-queue can be separated by whitespace. (default: all accessed)
  -c, --cpu <int>       max cpu number used. (default: 1)
  -m, --memory <int/str>
                        max memory used (GB). (default: 1G)
  --node [<node> ...]   node for running, multi-node can be separated by whitespace. (default: all accessed)
  --round-node          round all define node per job for load balance
```

##### runsge/runshell:

```
$ runsge --help
Usage: runsge [-h] [-v] [-j [<jobfile>]] [-n <int>] [-s <int>] [-e <int>] [-w <workdir>] [-d] [-l <file>] [-f] [-M {local,localhost,sge,slurm}] [--config <configfile>] [--dag]
              [--dag-extend] [--strict] [--quiet] [--show-config] [-r <int>] [-R <int>] [--max-check <float>] [--max-submit <float>] [--max-queue-time <float/str>]
              [--max-run-time <float/str>] [--max-wait-time <float/str>] [--max-timeout-retry <int>] [--local | --localhost | --sge | --slurm] [-N <jobname>] [-L <logdir>]
              [-g <int>] [--init <cmd>] [--callback <cmd>] [-q [<queue> ...]] [-c <int>] [-m <int/str>] [--node [<node> ...]] [--round-node]

runsge is a tool for managing parallel tasks from a specific shell file running in localhost, sge, slurm.

Options:
  -h, --help            show this help message and exit
  --local               submit your jobs to local, same as '--mode local'.
  --localhost           submit your jobs to localhost, same as '--mode localhost'.
  --sge                 submit your jobs to sge, same as '--mode sge'.
  --slurm               submit your jobs to slurm, same as '--mode slurm'.
  -N, --jobname <jobname>
                        job name. (default: basename of the jobfile)
  -L, --logdir <logdir>
                        the output log dir. (default: "<prog>_*_log_dir" under workdir)
  -g, --groups <int>    N lines to consume a new job group. (default: 1)
  --init <cmd>          command before all jobs, will be running in localhost.
  --callback <cmd>      command after all jobs finished, will be running in localhost.

Base Arguments:
  -v, --version         show program's version number and exit
  -j, --jobfile [<jobfile>]
                        input jobfile, if empty, stdin is used. (required)
  -n, --num <int>       the max job number running at the same time. (default: all of the jobfile, max 1000)
  -s, --start <int>     which line number (1-base) be used for the first job. (default: 1)
  -e, --end <int>       which line number (include) be used for the last job. (default: last line of the jobfile)
  -w, --workdir <workdir>
                        work directory. (default: current working directory)
  -d, --debug           log debug info.
  -l, --log <file>      append log info to file. (default: stdout)
  -f, --force           force to submit jobs even already succeeded.
  -M, --mode {local,localhost,sge,slurm}
                        the mode to submit your jobs, auto detect. (default: auto)
  --config <configfile>
                        input configfile for configurations search.
  --dag                 do not execute anything and print the directed acyclic graph of jobs in the dot language.
  --dag-extend          do not execute anything and print the extend directed acyclic graph of jobs in the dot language.
  --strict              use strict to run, means if any errors, clean all jobs and exit.
  --quiet               suppress all output and logging.
  --show-config         show configurations and exit.

Rate Arguments:
  -r, --retry <int>     retry N times of the error job, 0 or minus means do not re-submit. (default: 0)
  -R, --retry-sec <int>
                        retry the error job after N seconds. (default: 2)
  --max-check <float>   maximum number of job status checks per second, fractions allowed. (default: 5)
  --max-submit <float>  maximum number of jobs submitted per second, fractions allowed. (default: 20)

Time Arguments:
  --max-queue-time <float/str>
                        maximum time (d/h/m/s) between submit and running per job. (default: no-limiting)
  --max-run-time <float/str>
                        maximum time (d/h/m/s) start from running per job. (default: no-limiting)
  --max-wait-time <float/str>
                        maximum time (d/h/m/s) start from submit per job. (default: no-limiting)
  --max-timeout-retry <int>
                        retry N times for the timeout error job, 0 or minus means do not re-submit. (default: 0)

Resource Arguments:
  -q, --queue [<queue> ...]
                        queue/partition for running, multi-queue can be separated by whitespace. (default: all accessed)
  -c, --cpu <int>       max cpu number used. (default: 1)
  -m, --memory <int/str>
                        max memory used (GB). (default: 1G)
  --node [<node> ...]   node for running, multi-node can be separated by whitespace. (default: all accessed)
  --round-node          round all define node per job for load balance
```

##### qs/qslurm:

```
$ qs --help
query local/sge/slurm jobs.

Usage:
    qs [jobfile|logdir|logfile]
    qslurm
```

## File Formats

runjob supports three job file formats:

### 1. Flow format (runflow)

YAML-like syntax with dependency support, environment variable expansion, and job groups:

```
$ cat example2.flow

logs: ./

jobA:
    force: 1
    args: -q all.q -l vf=1g,p=1
    echo hello from job jobA  // --local
    echo 111

job:
    name jobB
    options -q all.q -l vf=1g,p=1
    echo hello from job jobB

task:
    name: jobC
    qsub_args -q all.q -l vf=1g,p=1
    echo hello from job jobC  // -f

jobD:
    sched_options -q all.q -l vf=1g,p=1
    cmd: echo hello from job jobD
    depends: jobB, jobC

jobB : jobA
jobD depends on jobB, jobC
```

### 2. Job format (runjob)

Traditional job_begin/job_end syntax with explicit dependency ordering:

```
$ cat example1.job

log_dir ./

job_begin
    name jobA
    sched_options -q all.q -l vf=1g,p=1
    force 0
    cmd_begin
        echo hello from job jobA
    cmd_end
job_end

job_begin
    name jobB
    sched_options -q all.q -l vf=1g,p=1
    cmd_begin
        echo hello from job jobB
    cmd_end
job_end

order jobB after jobA
```

### 3. Shell format (runsge/runshell)

Plain shell commands, use `wait` to separate job groups:

```
$ cat example.sh

echo hello
echo hello
echo runjob
echo runjob
wait
echo runjob // -m 1 -c 1
```

Each line becomes a job. Lines between `wait` markers form dependency groups — jobs after a `wait` depend on all jobs before it. The `//` suffix passes per-job options (e.g. `-m 1` for memory, `-c 1` for CPU).

## DAG Visualization

command `runflow -j test.flow --dag | dot -Tsvg > test.svg` will get the job graph:

![test](https://github.com/yodeng/runjob/assets/18365846/4f628b3e-4216-47c1-9287-9525639a9e9b)

## License

runjob is distributed under the [MIT license](./LICENSE).

## Contact

Please send comments, suggestions, bug reports and bug fixes to
yodeng@tju.edu.cn.

## Todo

More functions will be improved in the future.
