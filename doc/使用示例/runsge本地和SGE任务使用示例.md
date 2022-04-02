# runsge本地和SGE任务使用示例

runsge任务投递到SGE集群和在本地运行基本一样，只是改变运行参数为localhost即可，投递任务并行运行，通过参数控制同时运行的最大任务数，使用也不需要配置文件。

##### 1. 使用示例

准备shell文件，文件中的每行命令会作为一个任务投递到计算资源中，示例如下：

```
echo hello
echo runjob
wait
sleep 30 // -m 1 --cpu 1 --mode localhost
```

使用命令`runsge -j test.sh -m sge -l test.log  `即可将任务投递到计算资源中。



##### 2. 使用说明

+ 上述示例中第一行和第二行两个任务并发到SGE集群运行，第四行任务会等待第一、二行任务运行完成之后进行投递，由于后面参数指定了`--mode localhost`， 第四行任务会在本地运行。
+ 运行过程中可以通过查看`test.log`文件查看任务运行情况，或者通过`qs test.log`命令统计任务运行状态
+ `runsge`命令可以后台`nohup`挂起，避免意外中断.
+ 终止`runsge`进程，会中断投递，同时清空正在运行中的任务。
+ `runsge`支持`--init`和`--call-back`，用于在任务运行前或运行完成后执行相关，在本地执行相关命令。
+ 使用`runsge -m local`命令即可将任务投递到本地服务器运行

