# runjob

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=small)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_small)
[![PyPI version](https://img.shields.io/pypi/v/runjob.svg?logo=pypi&logoColor=FFE873)](https://pypi.python.org/pypi/runjob)
[![Downloads](https://static.pepy.tech/badge/runjob)](https://pepy.tech/project/runjob)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](https://anaconda.org/bioconda/runjob)

## Summary

runjob is a program for managing a group of related jobs running on a compute cluster `localhost`, [Sun Grid Engine](http://star.mit.edu/cluster/docs/0.93.3/guides/sge.html), [BatchCompute](https://help.aliyun.com/product/27992.html) .  It provides a convenient method for specifying dependencies between jobs and the resource requirements for each job (e.g. memory, CPU cores). It monitors the status of the jobs so you can tell when the whole group is done. Litter cpu or memory resource is used in the login compute node.

## OSCS

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=large)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_large)

## Software Requirements

python >=3.5

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

##### runjob：

	$ runjob --help 
	usage: runjob [-h] [-v] [-j [<jobfile>]] [-n <int>] [-s <int>] [-e <int>] [-d] [-l <file>] [-r <int>] [-ivs <int>] [-f] [--dot] [--local] [--strict]
	              [--quiet] [--max-check <float>] [--max-submit <float>] [--max-queue-time <float/str>] [--max-run-time <float/str>]
	              [--max-wait-time <float/str>] [--max-timeout-retry <int>] [-i [<str> ...]] [-m {sge,local,localhost}]
	
	runjob is a tool for managing parallel tasks from a specific job file running in localhost or sge cluster.
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -i, --injname [<str> ...]
	                        job names you need to run. (default: all job names of the jobfile)
	  -m, --mode {sge,local,localhost}
	                        the mode to submit your jobs, if no sge installed, always localhost. (default: sge)
	
	base arguments:
	  -v, --version         show program's version number and exit
	  -j, --jobfile [<jobfile>]
	                        input jobfile, if empty, stdin is used. (required)
	  -n, --num <int>       the max job number runing at the same time. (default: all of the jobfile, max 1000)
	  -s, --startline <int>
	                        which line number(1-base) be used for the first job. (default: 1)
	  -e, --endline <int>   which line number (include) be used for the last job. (default: last line of the jobfile)
	  -d, --debug           log debug info.
	  -l, --log <file>      append log info to file. (default: stdout)
	  -r, --retry <int>     retry N times of the error job, 0 or minus means do not re-submit. (default: 0)
	  -ivs, --retry-ivs <int>
	                        retry the error job after N seconds. (default: 2)
	  -f, --force           force to submit jobs even if already successed.
	  --dot                 do not execute anything and print the directed acyclic graph of jobs in the dot language.
	  --local               submit your jobs in localhost, same as '--mode local'.
	  --strict              use strict to run, means if any errors, clean all jobs and exit.
	  --quiet               suppress all output and logging.
	  --max-check <float>   maximal number of job status checks per second, fractions allowed. (default: 3)
	  --max-submit <float>  maximal number of jobs submited per second, fractions allowed. (default: 30)
	
	time control arguments:
	  --max-queue-time <float/str>
	                        maximal time (d/h/m/s) between submit and running per job. (default: no-limiting)
	  --max-run-time <float/str>
	                        maximal time (d/h/m/s) start from running per job. (default: no-limiting)
	  --max-wait-time <float/str>
	                        maximal time (d/h/m/s) start from submit per job. (default: no-limiting)
	  --max-timeout-retry <int>
	                        retry N times for the timeout error job, 0 or minus means do not re-submit. (default: 0)

##### runsge/runshell/runbatch:

```
$ runsge --help 
usage: runsge [-h] [-v] [-j [<jobfile>]] [-n <int>] [-s <int>] [-e <int>] [-d] [-l <file>] [-r <int>] [-ivs <int>] [-f] [--dot] [--local] [--strict]
              [--quiet] [--max-check <float>] [--max-submit <float>] [--max-queue-time <float/str>] [--max-run-time <float/str>]
              [--max-wait-time <float/str>] [--max-timeout-retry <int>] [-wd <workdir>] [-N <jobname>] [-lg <logdir>] [-g <int>] [--init <cmd>]
              [--call-back <cmd>] [--mode {sge,local,localhost,batchcompute}] [-ini <configfile>] [-config] [-q [<queue> ...]] [-m <int>] [-c <int>]
              [-om <dir>] [--access-key-id <str>] [--access-key-secret <str>]
              [--region {beijing,hangzhou,huhehaote,shanghai,zhangjiakou,chengdu,hongkong,qingdao,shenzhen}]

runsge is a tool for managing parallel tasks from a specific shell scripts runing in localhost, sge or batchcompute.

optional arguments:
  -h, --help            show this help message and exit
  -wd, --workdir <workdir>
                        work dir. (default: $PWD)
  -N, --jobname <jobname>
                        job name. (default: basename of the jobfile)
  -lg, --logdir <logdir>
                        the output log dir. (default: "$PWD/runsge_*_log_dir")
  -g, --groups <int>    N lines to consume a new job group. (default: 1)
  --init <cmd>          command before all jobs, will be running in localhost.
  --call-back <cmd>     command after all jobs finished, will be running in localhost.
  --mode {sge,local,localhost,batchcompute}
                        the mode to submit your jobs, if no sge installed, always localhost. (default: sge)
  -ini, --ini <configfile>
                        input configfile for configurations search.
  -config, --config     show configurations and exit.

base arguments:
  -v, --version         show program's version number and exit
  -j, --jobfile [<jobfile>]
                        input jobfile, if empty, stdin is used. (required)
  -n, --num <int>       the max job number runing at the same time. (default: all of the jobfile, max 1000)
  -s, --startline <int>
                        which line number(1-base) be used for the first job. (default: 1)
  -e, --endline <int>   which line number (include) be used for the last job. (default: last line of the jobfile)
  -d, --debug           log debug info.
  -l, --log <file>      append log info to file. (default: stdout)
  -r, --retry <int>     retry N times of the error job, 0 or minus means do not re-submit. (default: 0)
  -ivs, --retry-ivs <int>
                        retry the error job after N seconds. (default: 2)
  -f, --force           force to submit jobs even if already successed.
  --dot                 do not execute anything and print the directed acyclic graph of jobs in the dot language.
  --local               submit your jobs in localhost, same as '--mode local'.
  --strict              use strict to run, means if any errors, clean all jobs and exit.
  --quiet               suppress all output and logging.
  --max-check <float>   maximal number of job status checks per second, fractions allowed. (default: 3)
  --max-submit <float>  maximal number of jobs submited per second, fractions allowed. (default: 30)

time control arguments:
  --max-queue-time <float/str>
                        maximal time (d/h/m/s) between submit and running per job. (default: no-limiting)
  --max-run-time <float/str>
                        maximal time (d/h/m/s) start from running per job. (default: no-limiting)
  --max-wait-time <float/str>
                        maximal time (d/h/m/s) start from submit per job. (default: no-limiting)
  --max-timeout-retry <int>
                        retry N times for the timeout error job, 0 or minus means do not re-submit. (default: 0)

sge arguments:
  -q, --queue [<queue> ...]
                        the queue your job running, multi queue can be sepreated by whitespace. (default: all accessed queue)
  -m, --memory <int>    the memory used per command (GB). (default: 1)
  -c, --cpu <int>       the cpu numbers you job used. (default: 1)

batchcompute arguments:
  -om, --out-maping <dir>
                        the oss output directory if your mode is "batchcompute", all output file will be mapping to you OSS://BUCKET-NAME. if not set,
                        any output will be reserved.
  --access-key-id <str>
                        AccessKeyID while access oss.
  --access-key-secret <str>
                        AccessKeySecret while access oss.
  --region {beijing,hangzhou,huhehaote,shanghai,zhangjiakou,chengdu,hongkong,qingdao,shenzhen}
                        batch compute region. (default: beijing)
```

##### qs/qcs:

```
$ qs --help 
For summary all jobs
Usage: qs [jobfile|logdir|logfile]
       qcs --help
```



## License

runjob is distributed under the [MIT license](./LICENSE).

## Contact

Please send comments, suggestions, bug reports and bug fixes to
yodeng@tju.edu.cn.

## Todo

More functions will be improved in the future.
