## runbatch阿里云批量计算使用示例

分别基于阿里云实例自定义镜像和集运docker镜像进行说明

### 1. 基于阿里云实例镜像运行

 #### 1.1. 镜像构建

镜像构建须联系管理员，分配一台阿里云主机，在该主机上部署相关软件流程环境，并创建镜像。

#### 1.2. 配置文件和任务执行

##### 1.2.1 在集群上安装好`runjob`软件，配置好`$HOME/.runjob.config`配置文件，示例如下

```
[Cluster]
ImageId = img-ubuntu
InstanceType = 
system_disk = 
CidrBlock = 
VpcId = 


[Task]
timeout = 10800
stdoutlog = oss://xxxx/xxxx/logs/stdout/
stderrlog = oss://xxxx/xxx/logs/stderr/
DOCKER_IMAGE = 
DOCKER_REGISTRY_OSS_PATH = 


[OSS]
ossSrc = oss://xxxx/
mountPath = /share/oss/
access_key_id = xxxxx
access_key_secret = xxxxx
```

+ 配置文件中`xxxx`位置根据实际情况添加，本例中oss假设挂载到阿里云主机中的/share/oss/目录下，若分析流程中使用的数据库或者测试数据，也需要先上传到OSS的`/share/oss/xxx/xxx`的某个路径中

##### 1.2.2 任务运行

例如有一个任务：

现有一个数据库文件，存放在`/share/oss/dengyong/mytest.txt`，

需要在集群上运行任务，要求将数据文件中的内容读取出来，并写入到`/share/oss/dengyong/output/out.mytext`文件中

然后将`/share/oss/dengyong/output/out.mytext`该文件从`oss`上拷贝到本地主机

则集群上的`/home/dengyong/test/test.sh`脚本中内容为：

```shell
mkdir -p /share/oss/dengyong/output/   // -m 1 -c 1
wait
cat /share/oss/dengyong/mytest.txt > /share/oss/dengyong/output/out.mytext   // -m 1 -c 1 -om /share/oss/dengyong/output/
```

当前目录为`/home/dengyong/test/`使用命令如下：

```shell
runbatch -j test.sh --mode batchcompute --call-back "ossutil64 cp oss://xxxx/dengyong/output/out.mytext /home/dengyong/test/"
```

即可将任务投递到阿里云上，等待任务运行完成之后，会在`OSS`存储上生成一个`/share/oss/dengyong/output/out.mytext`文件，如果需要的话，使用

`--call-back`参数，通过`ossutil64`命令将`OSS`上的数据拷贝到本地服务器。

> 注：
>
> + 必须使用 `-om`参数，确保阿里云服务器程序运行完成之后，将数据回传到指定的OSS目录，该目录下的全部文件才能被持久化写入到OSS存储中
> + `/home/dengyong/test/test.sh`本实例中会投递两个任务，通过`wait`关键字定义依赖，先创建出文件夹，然后将数据重定向到输出文件。第二个任务中指定了`-om`，即保存到OSS上数据的路径。
> + `/home/dengyong/test/test.sh`也可以直接一行命令`mkdir -p /share/oss/dengyong/output/ &&  cat /share/oss/dengyong/mytest.txt > /share/oss/dengyong/output/out.mytext // -m 1 -c 1 --om /share/oss/dengyong/output/ `
> + 每行命令中指定的`//`后面的参数也可以通过`runbatch -m 1 -c 1 --om /share/oss/dengyong/output/`传入



### 2. 基于docker镜像运行

由于使用阿里云服务器的场景通常是大量计算和流程分析。流程环境复杂，需要通过本地部署和安装流程环境，docker作为一种通用虚拟的环境封装形式，是十分适用的场景。

 #### 2.1. 镜像构建

环境部署可直接在本地docker虚拟机中进行，需要部署环境时，可以联系我开放账户和密码远程连接到北京集群的虚拟主机中，进行环境部署和部署测试。

测试完成后将虚拟主机打包成docker镜像，并上传到OSS私有仓库中，北京集群也已搭建了OSS的私有docker仓库，可通过docker push命令将镜像上传。

上传后需要在配置文件中添加`DOCKER_IMAGE = `和`DOCKER_REGISTRY_OSS_PATH = `，或者单独添加一个参数配置文件，通过`runbatch --ini`参数将配置文件传入

#### 2.2. 配置文件和任务执行

##### 2.2.1 在集群上安装好`runjob`软件，配置好`$HOME/.runjob.config`配置文件，示例如下

```
[Cluster]
ImageId = img-ubuntu
InstanceType = 
system_disk = 
CidrBlock = 
VpcId = 


[Task]
timeout = 10800
stdoutlog = oss://xxxx/xxxx/logs/stdout/
stderrlog = oss://xxxx/xxx/logs/stderr/
DOCKER_IMAGE = localhost:5000/testimage:latest
DOCKER_REGISTRY_OSS_PATH = oss://xxxx/dengyong/dockers


[OSS]
ossSrc = oss://xxxx/
mountPath = /share/oss/
access_key_id = xxxxx
access_key_secret = xxxxx
```

+ 配置文件中`xxxx`位置根据实际情况添加，本例中oss假设挂载到阿里云主机中的`/share/oss/`目录下，若分析流程中使用的数据库或者测试数据，也需要先上传到OSS的`/share/oss/xxx/xxx`的某个路径中。
+ 须填写上传到OSS仓库中的docker镜像名和镜像路径。
+ 使用docker镜像运行时，`Cluster`的`ImageId`填写默认的`img-ubuntu`即可。

##### 2.2.2 任务运行

上述配置完成后，任务运行和投递和`1.2.2`中一致

