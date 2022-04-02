# runjob本地和SGE任务使用示例

runjob是job格式的任务文件投递到SGE或本地运行的命令

##### 1. 使用示例

准备job任务文件，文件中的每个任务会根据依赖关系投递到计算资源中

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
    sched_options -q all.q -l vf=1g,p=1
    cmd_begin
        echo hello from job jobC
    cmd_end
job_end


order jobC after jobA jobB
```

使用命令`runjob -j test.job -m sge -l test.log` 即可将任务投递到计算资源中。



##### 2. 使用说明

+ 上述示例中`jobA`和`jobB`两个任务并发到SGE集群运行，`jobC`任务会等待`jobA`和`jobB`运行完成之后进行投递，由于`jobC`指定了`host localhost`，所以`jobC`会在本地运行。
+ 运行过程中可以通过查看`test.log`文件查看任务运行情况，或者通过`qs test.log`命令统计任务运行状态。
+ `runjob`命令可以后台`nohup`挂起，避免意外中断。
+ 终止`runjob`进程，会中断投递，同时清空正在运行中的任务。
+ 使用`runjob -m local`命令即可将`test.job`文件中的任务投递到本地服务器运行。



