#### runjob任务调度

`runjob`是早期开发的用于SGE集群和本地任务投递、调度、监控的API。用于管理类似于sjm任务格式的job任务，基于python语言实现了其功能，完全兼容sjm的job输入文件，并结合实际生物信息应用场景，做了很多使用优化。

由于job文件格式简单且容易理解，因此在生信流程管理运行中也比较常用。`runjob`读取job输入文件，根据文件中指定的job格式规则，将任务分发到本地服务器或sge集群运行。

##### 主要特点

+ 每个任务对应一个唯一的日志文件，记录了任务运行的输出和状态。

+ 实时的任务日志和状态监控记录，维护了动态的运行时的任务队列。
+ 某个任务失败报错可重新投递运行。
+ 程序运行中断，重新执行只需要和第一次运行的命令一样即可，不需要进行改动。
+ 每次程序运行会自动跳过已经运行成功的任务。
+ 程序中断或被杀掉时，可以自动清理正在运行中的任务，避免计算资源的浪费和流程运行步骤的不确定。
+ 可实时查看当前流程运行到那些步骤以及其运行状态，掌握任务进度。
+ 支持只运行job文件中指定任务名匹配的任务，能快速识别和根据依赖关系自动投递指定任务。
+ ......



##### 参数说明

输入命令`runjob --help`可查看参数配置，各参数介绍如下：

| 参数            | 描述                                                         |
| --------------- | ------------------------------------------------------------ |
| -h/--help       | 打印帮助信息并退出                                           |
| -n/--num        | 同时运作的最大任务数，默认 1000                              |
| -j/--jobfile    | 输入的job流程任务文件                                        |
| -i/--injname    | 只运行job文件中匹配任务名的任务，多个匹配用空白隔开，默认全部任务 |
| -s/--start      | 只运行job文件中从指定行开始后面的任务，默认第0行             |
| -e/--end        | 只运行job文件中从指定行之前的任务，默认最后一行              |
| -r/--retry      | 某个任务运行失败会自动重新投递任务，默认投递3次。            |
| -ivs/--retry-ivs | 任务错误后，重投的等待时间，单位秒，默认2秒                  |
| -m/--mode       | 任务运行在本地还是在SGE集群，可选则`sge,local,localhost`，非集群环境自动进行本地运行。目前只支持sge和本地，后续可能会更新增加阿里云运行 |
| -nc/--noclean   | 流程中断退出或被杀掉时，选择是否情况投递上和正在运行的任务，默认自动清理。 |
| --strict        | 当流程中有一个任务重投运行--retry次数依然报错的时候，不再进行后续任务投递 |
| -d/--debug      | 程序运行日志级别debug，默认info                              |
| -l/--log        | 程序运行时输出的日志文件，默认屏幕输出                       |
| -v/--version    | 打印软件版本并退出                                           |



##### job输入文件格式说明

`runjob`输入的job文件格式和sjm兼容，格式规范如下：

```
log_dir /path/to/logdir

job_begin
    name jobA
    host localhost
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
    sched_options -q all.q -l vf=1g,p=1
    cmd_begin
        echo hello from job jobC
    cmd_end
job_end

...

order jobC after jobA jobB
# order jobB after jobA
# order jobC after jobA
# order jobA before jobC
```

+ `log_dir`指定流程运行的任务输出日志文件夹
+ `job_begin`和`job_end`代表了一个任务的开始和结束
+ `name`定义任务的名字
+ `host`指定任务运行的方式，本地运行或是sge投递，默认由--mode参数指定
+ `sched_options`定义sge运行的任务`qsub`投递参数，本地运行模式下无效
+ `cmd_begin`和`cmd_end`指定任务的运行的命令
+ `order ... after ...`定义了任务的依赖关系，多个依赖，任务名空白隔开即可
+ `order ... before ...`表示后面任务依赖前面任务
+ `#`开头为注释



##### 流程进度查看

任务状态和任务运行进度可以通过查看输出日志文件或者通过`qs`命令进行查看，可参考[qs集群和本地任务查看](./qs集群和本地任务查看.md)。
