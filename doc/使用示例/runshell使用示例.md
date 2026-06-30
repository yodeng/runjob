# runshell 本地和集群任务使用示例

`runshell` 任务投递到集群和本地运行基本一样，改变运行参数即可，投递任务并行运行，通过参数控制同时运行的最大任务数。

##### 1. 使用示例

准备shell文件，文件中每行命令作为一个任务投递：

```
echo hello
echo world
wait
sleep 30 // -m 1G --cpu 1 --mode localhost
```

使用命令投递任务：

```shell
runshell -j test.sh -M sge -l test.log
```

##### 2. 使用说明

+ 上述示例中前两行并发投递到SGE集群运行，第四行（`sleep 30`）等待前两个任务完成后投递，且因 `// --mode localhost` 指定本地运行。
+ `runshell` 命令可以后台 `nohup` 挂起，避免意外中断。
+ 终止 `runshell` 进程会自动清理正在运行中的任务。
+ 使用 `runshell -M local` 即可将任务投递到本地服务器运行。
+ `runshell` 支持 `--init` 和 `--callback` 用于任务前/后的本地操作。
+ 使用 `qs` 可以查看任务运行状态。

