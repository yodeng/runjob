[English](./README.md) | [中文](./README_zh.md)

# runjob

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=small)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_small)
[![PyPI version](https://img.shields.io/pypi/v/runjob.svg?logo=pypi&logoColor=FFE873)](https://pypi.python.org/pypi/runjob)
[![Downloads](https://static.pepy.tech/badge/runjob)](https://pepy.tech/project/runjob)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](https://anaconda.org/bioconda/runjob)

## Overview

`runjob` is a command-line tool for managing and scheduling groups of related jobs on compute clusters. It supports **localhost**, **Sun Grid Engine (SGE)**, and **Slurm** backends. It provides a convenient way to specify dependencies between jobs, define resource requirements (e.g., memory, CPU cores), and monitor job status in real time. You get a complete execution summary when all jobs finish. The scheduler itself uses negligible CPU and memory on the login node.

### Key Features

- **Multiple backends**: Seamless support for `local`, `localhost`, `SGE`, and `Slurm`, with automatic cluster detection
- **Three job file formats**: Flow format (YAML-like), Job format (traditional block definition), and Shell format (plain commands)
- **DAG dependency resolution**: Directed Acyclic Graph (DAG) based automatic dependency parsing with topological sorting
- **Environment variable expansion**: Define variables and perform Cartesian product expansion to generate parameter combinations in batch
- **Resource control**: Fine-grained per-job CPU, memory, queue/partition, and node allocation
- **Rate limiting**: Built-in RateLimiter to throttle job submission and status checks, preventing scheduler overload
- **Timeout mechanism**: Per-job queue timeout, run timeout, and total wait timeout with configurable retry
- **Failure retry**: Automatic job resubmission on failure with configurable retry count and interval
- **Job grouping**: Combine multiple command lines into a single job group
- **DAG visualization**: Output Graphviz DOT format dependency graphs for visualization
- **Signal handling**: Graceful handling of SIGINT/SIGTERM with automatic cleanup of all submitted jobs on exit
- **Persistent status**: Status tracking via marker files (`.success`/`.error`/`.submit`/`.run`)
- **Configuration system**: Multi-layer config file search with command-line, user-config, and package-config merging
- **Socket communication**: Unix Domain Socket and TCP Socket based job status notification
- **Job querying**: `qs`/`qslurm` commands for quick cluster job status overview

## Requirements

- Python >= 3.7
- Optional dependencies: `psutil`, `prettytable`, `rich-argparse`, `APScheduler`

## Installation

### Recommended (development version)

```bash
pip install git+https://github.com/yodeng/runjob.git
```

### PyPI (stable release)

```bash
pip install runjob -U
```

### Conda

```bash
conda install -c bioconda runjob
```

## Quick Start

### Available Commands

| Command | Description |
|---------|-------------|
| `runflow` | Execute Flow format job file (YAML-like syntax, supports dependencies and variable expansion) |
| `runjob` | Execute Job format job file (`job_begin/job_end` block syntax) |
| `runsge` / `runshell` | Execute Shell format job file (plain commands with `wait` barriers) |
| `qs` | Query local/SGE/Slurm job status |
| `qslurm` | Query Slurm cluster job status |
| `runjob-server` | Start the job status notification socket server |
| `runjob-client` / `runjob-report` | Send job status to the socket server |

### Example 1: Shell Format (simplest)

Create `example.sh`:

```bash
echo hello
echo hello
echo runjob
echo runjob
wait
echo runjob // -m 1 -c 1
```

Run:

```bash
runsge example.sh
# or
runshell example.sh
```

- The first 4 lines are submitted as 4 independent parallel jobs
- Jobs after `wait` only start after all preceding jobs complete
- The `//` suffix passes per-job arguments (e.g., `-m 1` for 1GB memory)

### Example 2: Job Format (traditional block syntax)

Create `example1.job`:

```
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

job_begin
    name jobD
    sched_options -q all.q -l vf=1g,p=1
    cmd_begin
        echo hello from job jobD
    cmd_end
job_end

order jobB after jobA
order jobD after jobB
```

Run:

```bash
runjob -j example1.job
```

### Example 3: Flow Format (recommended, most powerful)

Create `example2.flow`:

```yaml
logs: ./         ## define log directory

jobA:
    force: 1
    args: -q all.q -l vf=1g,p=1   ## SGE resource definition
    echo hello from job jobA  // --local   ## force local execution
    echo 111                       ## multi-line commands allowed

job:            ## "task:" or "job:" keyword both accepted
    name jobB    ## job name definition
    options -q all.q -l vf=1g,p=1
    echo hello from job jobB

task:
    name: jobC
    qsub_args -q all.q -l vf=1g,p=1
    echo hello from job jobC  // -f  ## force re-run regardless of success status

jobD:
    sched_options -q all.q -l vf=1g,p=1
    cmd: echo hello from job jobD
    depends: jobB, jobC              ## dependency definition

jobB : jobA                         ## concise dependency syntax
jobD depends on jobB, jobC          ## natural-language dependency syntax
```

Run:

```bash
runflow -j example2.flow
```

## Command Reference

### runflow

```
runflow [-h] [-v] [-j <jobfile>] [-n <int>] [-s <int>] [-e <int>] [-w <workdir>]
        [-d] [-l <file>] [-f] [-M {local,localhost,sge,slurm}]
        [--config <configfile>] [--dag] [--dag-extend] [--abort-on-error] [--quiet]
        [--show-config] [-r <int>] [-R <int>]
        [--max-check <float>] [--max-submit <float>]
        [--max-queue-time <float/str>] [--max-run-time <float/str>]
        [--max-wait-time <float/str>] [--max-timeout-retry <int>]
        [--local | --localhost | --sge | --slurm]
        [-i [<str> ...]] [-L <logdir>]
        [-q [<queue> ...]] [-c <int>] [-m <int/str>]
        [--node [<node> ...]] [--round-node]
```

#### Base Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `-j, --jobfile` | Input job file; reads from stdin if empty (**required**) | `stdin` |
| `-v, --version` | Show version number and exit | — |
| `-n, --num` | Maximum number of concurrent jobs | All (max 1000) |
| `-s, --start` | First line number to use (1-based) | 1 |
| `-e, --end` | Last line number to use (inclusive) | Last line |
| `-w, --workdir` | Working directory | Current directory |
| `-d, --debug` | Enable DEBUG level logging | `False` |
| `-l, --log` | Append log output to file | stdout |
| `-f, --force` | Force submission even if already succeeded | `False` |
| `-M, --mode` | Submission mode: `local`, `localhost`, `sge`, `slurm` | Auto-detect |
| `--config` | Configuration file path | — |
| `--dag` | Output DOT format DAG only, do not execute | `False` |
| `--dag-extend` | Output expanded DAG only, do not execute | `False` |
| `--abort-on-error` | Strict mode: terminate all jobs on any error | `False` |
| `--quiet` | Suppress all output and logging | `False` |
| `--show-config` | Show current configuration and exit | — |

#### Flow-specific Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `-i, --match` | Only run jobs matching names (glob/regex supported) | All jobs |
| `-L, --logdir` | Log output directory | `{workdir}/logs` |

#### Rate Control Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `-r, --retry` | Retry count for failed jobs (0 or negative = no retry) | 0 |
| `-R, --retry-sec` | Retry interval in seconds | 2 |
| `--max-check` | Max status checks per second (fractional values accepted) | 5 |
| `--max-submit` | Max job submissions per second (fractional values accepted) | 20 |

#### Timeout Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--max-queue-time` | Max time between submission and start (format: `d/h/m/s`, e.g., `2h30m`) | Unlimited |
| `--max-run-time` | Max time from job start to completion | Unlimited |
| `--max-wait-time` | Max total time from job submission | Unlimited |
| `--max-timeout-retry` | Retry count for timeout errors | 0 |

#### Resource Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `-q, --queue` | Queue/partition name(s), space-separated | All accessible |
| `-c, --cpu` | Max CPU cores per job | 1 |
| `-m, --memory` | Max memory per job (GB), e.g., `1G`, `512M` | 1G |
| `--node` | Target node(s), space-separated | All accessible |
| `--round-node` | Round-robin node assignment for load balancing | `False` |

### runsge / runshell

Shares base arguments with `runflow`, plus:

| Argument | Description | Default |
|----------|-------------|---------|
| `-N, --jobname` | Job name | Job file basename |
| `-L, --logdir` | Log output directory | `{workdir}/{prog}_*_log_dir` |
| `-g, --groups` | Merge N lines into one job group | 1 |
| `--init` | Command to run before all jobs (localhost) | — |
| `--callback` | Command to run after all jobs finish (localhost) | — |

### qs / qslurm

```bash
# Query all users' job status
qs

# Query status of a specific job file / log directory / log file
qs example1.job
qs ./logs
qs jobA.log

# Query Slurm cluster status
qslurm
```

## Job File Formats in Detail

### 1. Flow Format (`.flow` files)

The Flow format uses YAML-like syntax with support for dependency definitions, variable expansion, and rich job configuration.

#### Job Definition

Each job begins with an identifier followed by a colon; properties are defined with indentation.

**Job header keywords**:
- `job:` or `task:` — job block start marker
- `name` / `names` — job name (if omitted, the identifier before the colon is used)
- `force` — force re-execution (bypass success status), accepts `0`/`1` or `true`/`false`

**Resource definition keywords** (equivalent, choose one):
- `args:` / `options:` / `sched_options:` / `qsub_args:` — SGE qsub parameters

**Dependency definition keywords**:
- `depends:` / `depend:` — comma-separated list of dependent job names

**Host specification keywords**:
- `host:` — target backend (`local`/`sge`/`slurm`)
- `local:` / `locals:` / `localhost:` — boolean, force local execution

**Timeout configuration**:
- `max_queue_time` / `max_run_time` / `max_wait_time` / `max_timeout_retry` / `time`

**Command definition**:
- `cmd:` or inline commands — the commands to execute
- The `//` suffix passes per-job command-line arguments

#### Dependency Syntax (multiple styles supported)

```yaml
# Style 1: depends keyword
jobD:
    depends: jobB, jobC

# Style 2: colon syntax
jobB : jobA

# Style 3: natural language
jobD depends on jobB, jobC

# Style 4: arrow syntax
jobA -> jobB
jobC <- jobD
```

#### Variable Expansion

```yaml
envs:
    sample = A, B, C
    replicate = rep1=1, rep2=2, rep3=3

align:
    cmd: bwa mem ref.fa ${sample}.fq > ${sample}.${replicate}.sam
    depends: trim
```

The variable system supports:
- **List values**: `sample = A, B, C` — expands to an array of values
- **Dict values**: `replicate = rep1=1, rep2=2` — supports `.value` suffix to get all values
- **Cartesian product**: combining multiple variables generates all parameter combinations
- **Template syntax**: `${variable}` substitution in commands and filenames

#### External Includes

```yaml
include: common.flow    ## import another flow file, sharing its jobs and variables
```

### 2. Job Format (`.job` files)

The Job format uses `job_begin`/`job_end` blocks with commands inside `cmd_begin`/`cmd_end`.

```
log_dir ./                 ## optional: log directory

job_begin
    name jobA              ## job name (required)
    sched_options -q all.q -l vf=1g,p=1
    force 0
    cmd_begin
        echo hello from job jobA
        echo line 2         ## multi-line commands supported
    cmd_end
job_end

## dependency declarations (must follow all job_begin/job_end blocks)
order jobB after jobA
order jobD after jobB jobC    ## many-to-many dependencies supported
```

**Job block attributes**:

| Attribute | Description |
|-----------|-------------|
| `name` | Job name (required) |
| `sched_options` / `options` / `args` / `qsub_args` | SGE/Slurm scheduling parameters |
| `host` | Target backend |
| `force` | Force re-execution |
| `status` | Initial status |
| `depends` | Dependency job list |
| `memory` / `mem` | Memory requirement |
| `cpu` | CPU requirement |
| `queue` | Queue/partition |
| `extend` | Variable expansion list |
| `max_queue_time` / `max_run_time` / `max_wait_time` / `max_timeout_retry` / `time` | Timeout configuration |

### 3. Shell Format (`.sh` files)

The Shell format is the simplest — each line of commands becomes an independent job.

```bash
echo hello
echo hello
echo runjob
echo runjob
wait
echo runjob // -m 2G -c 2 --local
```

- `wait` — synchronization barrier; jobs after it wait for all preceding jobs to finish
- `// args` — per-job arguments at end of line; all `runsge` CLI arguments are accepted
- Each command line gets its own log file automatically

## Architecture

### Project Structure

```
runjob/
├── src/
│   ├── __init__.py       # Package entry, exports public API
│   ├── _version.py       # Version (v2.12.0)
│   ├── main.py           # CLI entry point dispatcher (entry_exec)
│   ├── run.py            # Core engine: RunJob (Shell format) / RunFlow (Flow/Job format)
│   ├── job.py            # Job object model: Job / Jobfile / Shellfile
│   ├── dag.py            # Directed Acyclic Graph (DAG) implementation
│   ├── scheduler.py      # APScheduler-based job scheduler
│   ├── parser.py         # CLI argument parser (argparse)
│   ├── config.py         # Configuration management (multi-layer merge)
│   ├── context.py        # Application-wide context (singleton)
│   ├── logger.py         # Logging system (with ANSI color support)
│   ├── limiter.py        # RateLimiter for submission/check throttling
│   ├── utils.py          # Utility function collection
│   ├── jobsocket.py      # Job status socket communication (Unix/TCP)
│   └── jobstat.py        # Job status query (qs/qslurm)
├── example/              # Example files
├── doc/                  # Documentation sources
├── recipe/               # Conda build recipe
└── setup.py              # Installation script
```

### Core Class Relationships

```
Context (singleton global context)
  └── Config (multi-layer configuration manager)
       └── CLI args / user config / package config

main.entry_exec()
  └── RunFlow (Flow/Job format) or RunJob (Shell format)
       ├── Jobfile / Shellfile (job file parsing)
       │    └── Job (single job object)
       ├── DAG (directed acyclic graph, dependency management)
       ├── JobQueue (ordered, deduplicated job queue)
       ├── RateLimiter (submission/check rate control)
       └── JobSocket (status notification socket)
```

### DAG Scheduling Flow

```
1. Parse job file → build list of Job objects
2. Parse dependencies → build DAG
3. Check already-succeeded jobs → remove from graph
4. Main scheduling loop:
   ├── Get independent nodes from DAG (no pending dependencies)
   ├── Throttle submission rate via RateLimiter
   ├── Submit jobs to target backend (local/SGE/Slurm)
   └── Background thread monitors job status
5. All jobs complete → output summary → cleanup resources
```

### Status Management

Each job's status is tracked via marker files on disk:

| Marker File | Meaning |
|-------------|---------|
| `.submit` | Job has been submitted |
| `.run` | Job is currently running |
| `.success` | Job completed successfully |
| `.error` | Job exited with an error |

State transitions:

```
wait → submit → run → success
                   → error → (retry) → submit → ...
                           → (exit/kill)
```

## DAG Visualization

Generate a job dependency graph:

```bash
runflow -j test.flow --dag | dot -Tsvg > test.svg
runflow -j test.flow --dag-extend | dot -Tsvg > test_extend.svg
```

- `--dag`: Output the current DAG; jobs from the same rule are colored identically
- `--dag-extend`: Output the expanded DAG (full graph after variable expansion)

![test](https://github.com/yodeng/runjob/assets/18365846/4f628b3e-4216-47c1-9287-9525639a9e9b)

## Advanced Features

### 1. Job Grouping

In Shell format, merge multiple lines into a single job group with `-g`:

```bash
runsge example.sh -g 3   # every 3 lines become one job
```

Commands within a group are joined with `&&`; any single failure fails the entire group.

### 2. Timeout Control

Per-job timeout configuration:

```bash
runflow -j jobs.flow --max-queue-time 2h --max-run-time 24h --max-wait-time 48h --max-timeout-retry 2
```

Supported time format: `2w 3d 8h 5m 2s` (weeks/days/hours/minutes/seconds)

Also configurable per-job in the job file:

```yaml
long_job:
    max_run_time: 48h
    max_queue_time: 4h
    cmd: ./long_running_script.sh
```

### 3. Round-Robin Node Load Balancing

```bash
runflow -j jobs.flow --node node01 node02 node03 --round-node
```

`--round-node` distributes jobs across specified nodes in round-robin order for simple load balancing.

### 4. Strict Mode

```bash
runflow -j jobs.flow --abort-on-error
```

Any job failure immediately terminates all running jobs and exits. Useful for workflows requiring atomic execution.

### 5. Automatic Detection and Fallback

- Auto-detects SGE environment (via `$SGE_ROOT` and `qconf`)
- Auto-detects Slurm environment (via `sinfo`/`sbatch`/`scancel`)
- Falls back to localhost mode when no cluster is detected
- In SGE mode, automatically discovers available queues and nodes

### 6. Configuration File System

Configuration search order (highest to lowest priority):

1. Command-line arguments
2. File specified by `--config`
3. User config file: `~/.config/runjob/config.ini`
4. Package config file: `{package}/config.ini`

View the current effective configuration:

```bash
runflow --show-config
```

### 7. Socket Status Notification

runjob supports external job status notification via sockets, enabling integration with other systems:

**Start server**:

```bash
runjob-server -f /tmp/runjob.sock        # Unix Domain Socket
runjob-server -H 0.0.0.0 -P 9999         # TCP Socket
```

**Send status**:

```bash
runjob-client -f /tmp/runjob.sock -n jobA -s success
runjob-client -H 127.0.0.1 -P 9999 -n jobA -s error
```

### 8. Retry Mechanism

```bash
runflow -j jobs.flow -r 3 -R 10   # retry up to 3 times with 10-second intervals
```

Retried jobs are marked `(re-submit)` in the log.

### 9. init / callback Hooks

```bash
runsge jobs.sh --init "echo 'All jobs starting...'" --callback "echo 'All jobs done!'"
```

- `--init`: Executed before all jobs (local mode)
- `--callback`: Executed after all jobs finish (local mode)

## API Usage

runjob can also be used as a Python library:

```python
from runjob import RunJob, RunFlow, Config, context

# Execute via Config object
config = Config()
config.update_dict(args={
    'jobfile': 'example.sh',
    'mode': 'slurm',
    'cpu': 4,
    'memory': '8G',
    'retry': 2,
})

with RunJob(config=config) as rj:
    rj.run()

# Or use RunFlow for Flow format
config.update_dict(args={'jobfile': 'example2.flow'})
with RunFlow(config=config) as rf:
    rf.run()
```

## User Guide

Full documentation is available at [runjob.readthedocs.io](https://runjob.readthedocs.io/en/latest/).

## OSCS Security

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=large)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_large)

## License

runjob is distributed under the [MIT license](./LICENSE).

## Contact

Please send comments, suggestions, bug reports, and bug fixes to yodeng@tju.edu.cn.

## Roadmap

More features will be added in future releases.
