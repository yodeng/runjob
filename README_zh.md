[English](./README.md) | [中文](./README_zh.md)

# runjob

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=small)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_small)
[![PyPI version](https://img.shields.io/pypi/v/runjob.svg?logo=pypi&logoColor=FFE873)](https://pypi.python.org/pypi/runjob)
[![Downloads](https://static.pepy.tech/badge/runjob)](https://pepy.tech/project/runjob)
[![install with bioconda](https://img.shields.io/badge/install%20with-bioconda-brightgreen.svg?style=flat)](https://anaconda.org/bioconda/runjob)

## 概述

`runjob` 是一个用于在计算集群上管理和调度批量任务的命令行工具，支持在 **本地 (localhost)**、**Sun Grid Engine (SGE)** 和 **Slurm** 三种环境下运行。它提供了便捷的方式来定义任务间的依赖关系、资源需求（如内存、CPU 核心数），并实时监控任务状态，你可以在所有任务完成后获得完整的执行摘要。整个调度过程在登录节点上几乎不消耗 CPU 和内存资源。

### 核心特性

- **多后端支持**：无缝支持 `local`、`localhost`、`SGE`、`Slurm` 四种运行模式，自动检测集群环境
- **三种任务文件格式**：Flow 格式 (YAML-like)、Job 格式 (传统块定义)、Shell 格式 (纯命令)
- **DAG 依赖管理**：基于有向无环图 (DAG) 自动解析任务依赖关系，支持拓扑排序调度
- **环境变量展开**：支持定义变量并对任务进行笛卡尔积展开，批量生成参数组合任务
- **资源控制**：精细控制每个任务的 CPU、内存、队列/分区、节点等资源分配
- **速率限制**：内置 RateLimiter 控制任务提交和状态检查的频率，避免对调度器造成压力
- **超时机制**：支持任务级别的队列等待超时、运行超时和总等待超时，可配置超时重试
- **失败重试**：支持任务失败后自动重试，可配置重试次数和重试间隔
- **任务分组**：支持将多行命令合并为一个任务组 (job group)
- **DAG 可视化**：支持输出 Graphviz DOT 格式的任务依赖图，方便可视化
- **信号处理**：优雅处理 SIGINT、SIGTERM 信号，退出时自动清理所有已提交任务
- **状态持久化**：通过标记文件 (`.success`/`.error`/`.submit`/`.run`) 追踪任务状态
- **配置系统**：多层配置文件搜索机制，支持命令行、用户配置、包配置合并
- **Socket 通信**：支持 Unix Domain Socket 和 TCP Socket 的任务状态通知
- **任务查询**：`qs`/`qslurm` 命令快速查看集群任务状态

## 软件要求

- Python >= 3.7
- 可选依赖：`psutil`、`prettytable`、`rich-argparse`、`APScheduler`

## 安装

### 推荐方式（开发版）

```bash
pip install git+https://github.com/yodeng/runjob.git
```

### PyPI 稳定版

```bash
pip install runjob -U
```

### Conda 安装

```bash
conda install -c bioconda runjob
```

## 快速开始

### 可用命令一览

| 命令 | 说明 |
|------|------|
| `runflow` | 执行 Flow 格式任务文件（YAML-like 语法，支持依赖和变量展开） |
| `runjob` | 执行 Job 格式任务文件（`job_begin/job_end` 块语法） |
| `runsge` / `runshell` | 执行 Shell 格式任务文件（纯命令，支持 `wait` 同步屏障） |
| `qs` | 查询本地/SGE/Slurm 集群任务状态 |
| `qslurm` | 查询 Slurm 集群任务状态 |
| `runjob-server` | 启动任务状态通知 Socket 服务端 |
| `runjob-client` / `runjob-report` | 向 Socket 服务端发送任务状态 |

### 示例 1：Shell 格式（最简单）

创建文件 `example.sh`：

```bash
echo hello
echo hello
echo runjob
echo runjob
wait
echo runjob // -m 1 -c 1
```

运行：

```bash
runsge example.sh
# 或
runshell example.sh
```

- 前 4 行作为 4 个独立任务并行提交
- `wait` 之后的任务等待前面所有任务完成后再执行
- `//` 后缀可以传递每任务级别的参数（如 `-m 1` 表示 1GB 内存，`-c 1` 表示 1 核 CPU）

### 示例 2：Job 格式（传统块定义）

创建文件 `example1.job`：

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

运行：

```bash
runjob -j example1.job
```

### 示例 3：Flow 格式（推荐，功能最强大）

创建文件 `example2.flow`：

```yaml
logs: ./         ## 指定日志目录

jobA:
    force: 1
    args: -q all.q -l vf=1g,p=1   ## SGE 资源定义
    echo hello from job jobA  // --local   ## 强制本地运行
    echo 111                       ## 支持多行命令

job:            ## "task:" 或 "job:" 关键字均可
    name jobB    ## 任务名称定义
    options -q all.q -l vf=1g,p=1
    echo hello from job jobB

task:
    name: jobC
    qsub_args -q all.q -l vf=1g,p=1
    echo hello from job jobC  // -f  ## 强制执行，忽略已有成功状态

jobD:
    sched_options -q all.q -l vf=1g,p=1
    cmd: echo hello from job jobD
    depends: jobB, jobC              ## 依赖定义

jobB : jobA                         ## 简洁的依赖语法
jobD depends on jobB, jobC          ## 自然语言风格的依赖语法
```

运行：

```bash
runflow -j example2.flow
```

## 命令详解

### runflow 命令参数

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

#### 基础参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-j, --jobfile` | 输入任务文件，为空时从 stdin 读取（**必填**） | `stdin` |
| `-v, --version` | 显示版本号 | — |
| `-n, --num` | 同时运行的最大任务数 | 全部（最大 1000） |
| `-s, --start` | 从第几行（1-based）开始计数 | 1 |
| `-e, --end` | 到第几行（包含）结束 | 最后一行 |
| `-w, --workdir` | 工作目录 | 当前目录 |
| `-d, --debug` | 输出 DEBUG 级别日志 | `False` |
| `-l, --log` | 将日志追加到文件 | stdout |
| `-f, --force` | 强制提交，即使任务已成功 | `False` |
| `-M, --mode` | 提交模式：`local`, `localhost`, `sge`, `slurm` | 自动检测 |
| `--config` | 指定配置文件 | — |
| `--dag` | 仅输出 DOT 格式的 DAG 图，不执行 | `False` |
| `--dag-extend` | 输出变量展开后的 DAG 图，不执行 | `False` |
| `--abort-on-error` | 严格模式：任一任务失败则清理所有任务并退出 | `False` |
| `--quiet` | 静默模式，抑制所有输出 | `False` |
| `--show-config` | 显示当前配置并退出 | — |

#### Flow 格式专用参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-i, --match` | 仅运行名称匹配的任务（支持 glob/正则） | 全部任务 |
| `-L, --logdir` | 日志输出目录 | `{workdir}/logs` |

#### 速率控制参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-r, --retry` | 失败任务重试次数（0 或负数表示不重试） | 0 |
| `-R, --retry-sec` | 失败任务重试间隔（秒） | 2 |
| `--max-check` | 每秒最大状态检查次数（支持小数） | 5 |
| `--max-submit` | 每秒最大提交任务数（支持小数） | 20 |

#### 超时控制参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--max-queue-time` | 任务从提交到运行的最大等待时间（格式：`d/h/m/s`，如 `2h30m`） | 不限制 |
| `--max-run-time` | 任务从开始运行的最大执行时间 | 不限制 |
| `--max-wait-time` | 任务从提交开始的最大总等待时间 | 不限制 |
| `--max-timeout-retry` | 超时任务的重试次数 | 0 |

#### 资源参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-q, --queue` | 指定队列/分区，支持多个（空格分隔） | 所有可访问队列 |
| `-c, --cpu` | 最大 CPU 核数 | 1 |
| `-m, --memory` | 最大内存（GB），支持如 `1G`、`512M` | 1G |
| `--node` | 指定运行节点，支持多个（空格分隔） | 所有可访问节点 |
| `--round-node` | 轮询分配节点实现负载均衡 | `False` |

### runsge / runshell 命令参数

与 `runflow` 共享基础参数，额外增加：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-N, --jobname` | 任务名称 | 任务文件的 basename |
| `-L, --logdir` | 日志输出目录 | `{workdir}/{prog}_*_log_dir` |
| `-g, --groups` | 每 N 行合并为一个任务组 | 1 |
| `--init` | 所有任务前执行的命令（在 localhost 运行） | — |
| `--callback` | 所有任务完成后执行的命令（在 localhost 运行） | — |

### qs / qslurm

```bash
# 查询所有用户的任务状态
qs

# 查询特定任务文件/日志目录/日志文件的状态
qs example1.job
qs ./logs
qs jobA.log

# 查询 Slurm 集群状态
qslurm
```

## 任务文件格式详解

### 1. Flow 格式（`.flow` 文件）

Flow 格式采用 YAML-like 语法，支持依赖定义、变量展开和丰富的任务配置，是推荐使用的格式。

#### 任务定义

每个任务由一个标识符后跟冒号开始，内部使用缩进定义属性。

**任务头部关键字**：
- `job:` 或 `task:` — 任务块起始标记
- `name` / `names` — 任务名称（如未指定，则使用冒号前的标识符作为名称）
- `force` — 是否强制执行（忽略已有成功状态），值为 `0`/`1` 或 `true`/`false`

**资源定义关键字**（效果等价，任选其一）：
- `args:` / `options:` / `sched_options:` / `qsub_args:` — SGE qsub 参数

**依赖定义关键字**：
- `depends:` / `depend:` — 逗号分隔的依赖任务列表

**主机指定关键字**：
- `host:` — 指定运行后端（`local`/`sge`/`slurm`）
- `local:` / `locals:` / `localhost:` — 布尔值，强制本地运行

**超时配置**：
- `max_queue_time` / `max_run_time` / `max_wait_time` / `max_timeout_retry` / `time`

**命令定义**：
- `cmd:` 或直接内联命令 — 任务的执行命令
- 可以使用 `//` 后缀传递每任务级别的命令行参数

#### 依赖语法（支持多种风格）

```yaml
# 风格 1：使用 depends 关键字
jobD:
    depends: jobB, jobC

# 风格 2：冒号语法
jobB : jobA

# 风格 3：自然语言
jobD depends on jobB, jobC

# 风格 4：箭头语法
jobA -> jobB
jobC <- jobD
```

#### 变量展开

```yaml
envs:
    sample = A, B, C
    replicate = rep1=1, rep2=2, rep3=3

align:
    cmd: bwa mem ref.fa ${sample}.fq > ${sample}.${replicate}.sam
    depends: trim
```

变量系统支持：
- **列表值**：`sample = A, B, C` — 直接展开为同名变量
- **字典值**：`replicate = rep1=1, rep2=2` — 支持 `.value` 后缀获取所有值
- **笛卡尔积展开**：多个变量组合时自动生成所有参数组合
- **模板语法**：`${variable}` 在命令和文件名中进行替换

#### 外部引用

```yaml
include: common.flow    ## 引用外部 flow 文件，共享其中的任务和变量定义
```

### 2. Job 格式（`.job` 文件）

Job 格式使用 `job_begin/job_end` 块定义，命令放在 `cmd_begin/cmd_end` 之间。

```
log_dir ./                 ## 可选：指定日志目录

job_begin
    name jobA              ## 任务名称（必填）
    sched_options -q all.q -l vf=1g,p=1
    force 0
    cmd_begin
        echo hello from job jobA
        echo line 2         ## 支持多行命令
    cmd_end
job_end

## 依赖声明（必须在所有 job_begin/job_end 块之后）
order jobB after jobA
order jobD after jobB jobC    ## 支持多对多依赖
```

**Job 块内属性**：

| 属性 | 说明 |
|------|------|
| `name` | 任务名称（必填） |
| `sched_options` / `options` / `args` / `qsub_args` | SGE/Slurm 调度参数 |
| `host` | 指定运行后端 |
| `force` | 强制执行 |
| `status` | 初始状态 |
| `depends` | 依赖任务列表 |
| `memory` / `mem` | 内存需求 |
| `cpu` | CPU 需求 |
| `queue` | 队列/分区 |
| `extend` | 变量展开列表 |
| `max_queue_time` / `max_run_time` / `max_wait_time` / `max_timeout_retry` / `time` | 超时配置 |

### 3. Shell 格式（`.sh` 文件）

Shell 格式最为简单，每行命令即为一个独立任务。

```bash
echo hello
echo hello
echo runjob
echo runjob
wait
echo runjob // -m 2G -c 2 --local
```

- `wait` — 同步屏障，之后的任务等待之前所有任务完成
- `// 参数` — 行尾参数传递，支持所有 `runsge` 命令行参数
- 每行命令自动生成独立的日志文件

## 架构设计

### 项目结构

```
runjob/
├── src/
│   ├── __init__.py       # 包入口，导出公共 API
│   ├── _version.py       # 版本号 (v2.12.1)
│   ├── main.py           # 命令行入口分发（entry_exec）
│   ├── run.py            # 核心引擎：RunJob（Shell 格式）/ RunFlow（Flow/Job 格式）
│   ├── job.py            # 任务对象模型：Job / Jobfile / Shellfile
│   ├── dag.py            # 有向无环图 (DAG) 实现
│   ├── scheduler.py      # 基于 APScheduler 的任务调度器
│   ├── parser.py         # 命令行参数解析器（argparse）
│   ├── config.py         # 配置管理系统（多层配置合并）
│   ├── context.py        # 应用全局上下文（单例模式）
│   ├── logger.py         # 日志系统（带 ANSI 颜色支持）
│   ├── limiter.py        # RateLimiter 速率控制器
│   ├── utils.py          # 工具函数集合
│   ├── jobsocket.py      # 任务状态 Socket 通信（Unix/TCP）
│   └── jobstat.py        # 任务状态查询（qs/qslurm）
├── example/              # 示例文件
├── doc/                  # 文档源文件
├── recipe/               # Conda 构建配方
└── setup.py              # 安装脚本
```

### 核心类关系

```
Context (单例全局上下文)
  └── Config (多层配置管理)
       └── 命令行参数 / 用户配置 / 包配置

main.entry_exec()
  └── RunFlow (Flow/Job 格式) 或 RunJob (Shell 格式)
       ├── Jobfile / Shellfile (任务文件解析)
       │    └── Job (单个任务对象)
       ├── DAG (有向无环图，管理依赖关系)
       ├── JobQueue (有序任务队列，自动去重)
       ├── RateLimiter (提交/检查速率控制)
       └── JobSocket (状态通知 Socket)
```

### DAG 调度流程

```
1. 解析任务文件 → 构建 Job 对象列表
2. 解析依赖关系 → 构建 DAG
3. 检查已成功任务 → 从图中移除
4. 主调度循环：
   ├── 获取 DAG 中无依赖的独立节点
   ├── 通过 RateLimiter 控制提交速率
   ├── 提交任务到对应后端 (local/SGE/Slurm)
   └── 后台线程监控任务状态
5. 所有任务完成 → 输出摘要 → 清理资源
```

### 状态管理

每个任务通过标记文件追踪状态：

| 标记文件 | 含义 |
|----------|------|
| `.submit` | 任务已提交 |
| `.run` | 任务正在运行 |
| `.success` | 任务成功完成 |
| `.error` | 任务执行出错 |

任务状态流转：

```
wait → submit → run → success
                   → error → (retry) → submit → ...
                           → (exit/kill)
```

## DAG 可视化

生成任务依赖图：

```bash
runflow -j test.flow --dag | dot -Tsvg > test.svg
runflow -j test.flow --dag-extend | dot -Tsvg > test_extend.svg
```

- `--dag`：输出当前 DAG 图，相同规则的任务用同色标记
- `--dag-extend`：输出变量展开后的完整 DAG 图

![test](https://github.com/yodeng/runjob/assets/18365846/4f628b3e-4216-47c1-9287-9525639a9e9b)

## 高级特性

### 1. 任务分组 (Job Groups)

在 Shell 格式中，可以通过 `-g` 参数将多行合并为一个任务组：

```bash
runsge example.sh -g 3   # 每 3 行合并为一个任务
```

任务组内的命令通过 `&&` 连接，任一行失败则整个组失败。

### 2. 超时控制

任务级别的超时配置：

```bash
runflow -j jobs.flow --max-queue-time 2h --max-run-time 24h --max-wait-time 48h --max-timeout-retry 2
```

支持的时间格式：`2w 3d 8h 5m 2s`（周/天/小时/分钟/秒）

也可以在任务文件中按任务配置：

```yaml
long_job:
    max_run_time: 48h
    max_queue_time: 4h
    cmd: ./long_running_script.sh
```

### 3. 节点轮询负载均衡

```bash
runflow -j jobs.flow --node node01 node02 node03 --round-node
```

`--round-node` 使任务轮流分配到指定节点，实现简单的负载均衡。

### 4. 严格模式

```bash
runflow -j jobs.flow --abort-on-error
```

任一任务失败立即终止所有运行中的任务并退出，适用于需要原子性执行的场景。

### 5. 自动检测与 Fallback

- 自动检测 SGE 环境（通过 `$SGE_ROOT` 和 `qconf`）
- 自动检测 Slurm 环境（通过 `sinfo`/`sbatch`/`scancel`）
- 未检测到集群时自动 fallback 到 localhost 模式
- SGE 模式下自动获取可用队列和节点列表

### 6. 配置文件系统

配置搜索顺序（优先级从高到低）：

1. 命令行参数
2. `--config` 指定的配置文件
3. 用户配置文件：`~/.config/runjob/config.ini`
4. 包配置文件：`{package}/config.ini`

查看当前生效的配置：

```bash
runflow --show-config
```

### 7. Socket 状态通知

runjob 支持通过 Socket 进行任务状态的外部通知，方便与其他系统集成：

**启动服务端**：

```bash
runjob-server -f /tmp/runjob.sock        # Unix Domain Socket
runjob-server -H 0.0.0.0 -P 9999         # TCP Socket
```

**发送状态**：

```bash
runjob-client -f /tmp/runjob.sock -n jobA -s success
runjob-client -H 127.0.0.1 -P 9999 -n jobA -s error
```

### 8. 重试机制

```bash
runflow -j jobs.flow -r 3 -R 10   # 失败后最多重试 3 次，间隔 10 秒
```

重试时会在日志中标记 `(re-submit)`。

### 9. init / callback 钩子

```bash
runsge jobs.sh --init "echo '所有任务开始...'" --callback "echo '所有任务完成！'"
```

- `--init`：在所有任务之前执行（local 模式）
- `--callback`：在所有任务之后执行（local 模式）

## API 使用

runjob 也可以作为 Python 库使用：

```python
from runjob import RunJob, RunFlow, Config, context

# 通过 Config 对象执行
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

# 或者使用 RunFlow 执行 Flow 格式
config.update_dict(args={'jobfile': 'example2.flow'})
with RunFlow(config=config) as rf:
    rf.run()
```

## 用户手册

完整文档请访问 [runjob.readthedocs.io](https://runjob.readthedocs.io/en/latest/)。

## OSCS 安全扫描

[![OSCS Status](https://www.oscs1024.com/platform/badge/yodeng/runjob.svg?size=large)](https://www.oscs1024.com/project/yodeng/runjob?ref=badge_large)

## 许可证

runjob 基于 [MIT license](./LICENSE) 分发。

## 联系方式

如有问题、建议或 Bug 报告，请发送至：yodeng@tju.edu.cn。

## 待办事项

更多功能将在未来版本中逐步完善。
