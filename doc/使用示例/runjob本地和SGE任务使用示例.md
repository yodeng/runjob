# runjob 本地和集群任务使用示例

`runjob` 是 job 格式任务文件投递到 SGE 集群、Slurm 集群或本地运行的命令。

##### 1. 使用示例

准备job任务文件，文件中的每个任务会根据依赖关系投递到计算资源中：

```
log_dir /path/to/logdir

job_begin
    name jobA
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

使用命令投递任务：

```shell
runjob -j test.job -M sge -l test.log
```

##### 2. 使用说明

+ 上述示例中 `jobA` 和 `jobB` 并发投递到集群运行，`jobC` 等待前两个完成后在本地运行。
+ 运行过程中可以通过 `qs test.log` 查看任务状态。
+ `runjob` 命令可以后台 `nohup` 挂起。
+ 终止 `runjob` 进程会自动清理正在运行中的任务。
+ 使用 `runjob -M local` 即可投递到本地服务器。
+ 支持 `--dag` 和 `--dag-extend` 输出任务依赖图。

