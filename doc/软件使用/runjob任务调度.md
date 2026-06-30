#### runjob 任务调度

`runjob`是用于SGE集群、Slurm集群和本地任务投递、调度、监控的工具。用于管理job格式的任务文件，基于python语言实现了其功能，并结合实际生物信息应用场景，做了很多使用优化。

`runjob`读取job输入文件，根据文件中指定的job格式规则，将任务分发到本地服务器、SGE集群或Slurm集群运行。`runflow` 命令功能与 `runjob` 完全一致。

##### 主要特点

+ 每个任务对应一个唯一的日志文件，记录了任务运行的输出和状态。
+ 实时的任务日志和状态监控记录，维护了动态的运行时的任务队列。
+ 某个任务失败报错可重新投递运行。
+ 程序运行中断，重新执行只需要和第一次运行的命令一样即可，不需要进行改动。
+ 每次程序运行会自动跳过已经运行成功的任务。
+ 程序中断或被杀掉时，可以自动清理正在运行中的任务，避免计算资源的浪费和流程运行步骤的不确定。
+ 支持 `--dag` 和 `--dag-extend` 输出任务依赖图，方便流程可视化。
+ 支持 `includes` 引入外部规则文件，复用任务定义。
+ 支持 `envs/vars` 变量展开，自动按笛卡尔积生成组合任务。
+ 支持 `extends` 继承已有任务模板。
+ ......



##### 参数说明

输入命令`runjob --help`可查看参数配置，各参数介绍如下：

| 参数 | 描述 |
| --------------- | ------------------------------------------------------------ |
| -h/--help | 打印帮助信息并退出 |
| -v/--version | 打印软件版本并退出 |
| -j/--jobfile | 输入的job流程任务文件（必填，默认从stdin读取） |
| -i/--injname | 只运行job文件中匹配任务名的任务，多个匹配用空白隔开，默认全部任务 |
| -s/--start | 只运行job文件中从指定行开始后面的任务，默认第1行 |
| -e/--end | 只运行job文件中到指定行之前的任务，默认最后一行 |
| -n/--num | 同时运行的最大任务数，默认全部（最大1000） |
| -M/--mode | 任务运行模式，可选 `local, localhost, sge, slurm`，默认自动检测 |
| -w/--workdir | 任务提交时的工作目录，默认为当前目录 |
| -L/--logdir | 各任务的日志输出文件夹，默认为 `workdir/runjob_<jobfile>_log_dir` |
| -q/--queue | SGE/Slurm 集群队列名，多个用空白隔开 |
| -c/--cpu | 任务使用的cpu资源申请，默认1 |
| -m/--memory | 任务使用的内存资源申请，默认1G |
| --node | 任务运行的集群节点，多个用空白隔开 |
| --round-node | 在所有指定节点间轮询分配任务 |
| -r/--retry | 任务运行失败后重新投递次数，默认0（不重投） |
| -R/--retry-sec | 任务错误后重投的等待时间，单位秒，默认2 |
| --max-check | 每秒最大检查任务状态次数，默认5 |
| --max-submit | 每秒最大投递任务数，默认20 |
| --max-queue-time | 任务从提交到开始运行的最大等待时间（支持d/h/m/s格式） |
| --max-run-time | 任务最大运行时间（支持d/h/m/s格式） |
| --max-wait-time | 任务从提交开始的最大存活时间（支持d/h/m/s格式） |
| --max-timeout-retry | 超时任务的重试次数，默认0 |
| --init | 所有任务开始之前运行的命令（本地执行） |
| --callback | 所有任务结束之后运行的命令（本地执行） |
| --dag | 输出任务依赖图（dot语言），不执行任务 |
| --dag-extend | 输出扩展任务依赖图（dot语言），不执行任务 |
| --abort-on-error | 严格模式，单个任务失败则终止所有任务并退出 |
| -f/--force | 强制重新提交已成功的任务 |
| --quiet | 静默模式，不输出日志 |
| -d/--debug | 程序日志级别debug，默认info |
| -l/--log | 程序日志输出文件，默认屏幕输出 |
| --config | 指定配置文件路径 |
| --show-config | 打印当前配置并退出 |



##### job输入文件格式说明

`runjob`输入的job文件格式规范如下：

```
log_dir /path/to/logdir

job_begin
    name jobA
    host sge
    sched_options -q all.q -l vf=1g,p=1
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
    name jobC
    host localhost
    cmd_begin
        echo hello from job jobC
    cmd_end
job_end

order jobC after jobA jobB
```

+ `log_dir` / `logdir` 指定流程运行的任务输出日志文件夹
+ `job_begin` 和 `job_end` 代表了一个任务的开始和结束
+ `name` 定义任务的名称
+ `host` 指定任务运行方式：`sge`、`slurm`、`local` 或 `localhost`
+ `sched_options` / `options` / `args` 定义集群投递参数：
  - `-c/--cpu N` 申请 N 个 CPU
  - `-m/--memory N` 申请内存
  - `-q/--queue NAME` 指定队列
  - `-l vf=MEM,p=CPU` 资源限定格式
+ `cmd_begin` 和 `cmd_end` 之间为任务运行的命令
+ `order ... after ...` 定义任务依赖关系
+ `order ... before ...` 等价于上述反向依赖
+ `A -> B` 或 `A : B` 或 `A <- B` 也可定义依赖关系
+ `#` 开头为注释

###### 扩展字段

+ `memory` / `mem` : 指定任务内存
+ `cpu` : 指定任务 CPU
+ `queue` : 指定任务队列
+ `depends` : 指定任务依赖，等价于 order 语句
+ `force` : 强制重新运行（`true/yes/1`）
+ `local` / `localhost` : 在本地运行（`true/yes/1`）
+ `status` : 任务初始状态
+ `max_queue_time` / `max_run_time` / `max_wait_time` : 超时控制
+ `max_timeout_retry` / `time` : 超时重试次数及时间
+ `extends` : 继承已有任务定义，自动展开变量
+ `cmd` : 单行命令（替代 cmd_begin/cmd_end 块）

###### 变量展开与模板继承

```
envs sample01
    S=A1 B1 C1
    LIB=lib1
envs sample01
    S=FP1 RP1

job_begin
    name mapping_${S}.${LIB}
    extends mapping
job_end

# 上面定义会自动展开为:
# mapping_A1.lib1, mapping_B1.lib1, mapping_C1.lib1,
# mapping_FP1.lib1, mapping_RP1.lib1
```

###### 外部规则引入

```
includes path/to/common_rules.job
```

通过 `includes` 可引入外部 job 规则文件，复用通用任务定义。



##### 流程进度查看

任务状态和任务运行进度可以通过查看输出日志文件或者通过`qs`命令进行查看，可参考 [qs集群和本地任务查看](./qs集群和本地任务查看.md)。

