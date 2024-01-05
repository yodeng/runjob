# !/usr/bin/env python
# coding: utf-8

import os
import re
import sys
import json
import string
import logging

from random import sample

from batchcompute import CN_BEIJING
from batchcompute import Client, ClientError
from batchcompute.resources import (
    Configs, Networks, VPC, AutoCluster,
    JobDescription, TaskDescription, DAG, Mounts,
)

from .context import context

REGION = {
    'BEIJING': 'batchcompute.cn-beijing.aliyuncs.com',
    'HANGZHOU': 'batchcompute.cn-hangzhou.aliyuncs.com',
    'HUHEHAOTE': 'batchcompute.cn-huhehaote.aliyuncs.com',
    'SHANGHAI': 'batchcompute.cn-shanghai.aliyuncs.com',
    'ZHANGJIAKOU': 'batchcompute.cn-zhangjiakou.aliyuncs.com',
    'CHENGDU': 'batchcompute.cn-chengdu.aliyuncs.com',
    'HONGKONG': 'batchcompute.cn-hongkong.aliyuncs.com',
    'QINGDAO': 'batchcompute.cn-qingdao.aliyuncs.com',
    'SHENZHEN': 'batchcompute.cn-shenzhen.aliyuncs.com'
}


class RClient(object):

    def __init__(self, config=None):
        self.conf = config
        self.client = config.client


class Cluster(RClient):

    def __init__(self, config=None):
        super(Cluster, self).__init__(config)
        cluster = AutoCluster()
        cluster.ImageId = self.conf.rget("Cluster", "ImageId")
        cluster.ReserveOnFail = False
        cluster.ResourceType = "OnDemand"
        cluster.InstanceType = self.conf.rget("Cluster", "InstanceType")
        configs = Configs()
        if self.conf.rget("Cluster", "CidrBlock") is not None and self.conf.rget("Cluster", "VpcId") is not None:
            networks = Networks()
            vpc = VPC()
            vpc.CidrBlock = self.conf.rget("Cluster", "CidrBlock")
            vpc.VpcId = self.conf.rget("Cluster", "VpcId")
            networks.VPC = vpc
            configs.Networks = networks
        if self.conf.rget("Cluster", "system_disk") is not None:
            configs.add_system_disk(size=int(self.conf.rget(
                "Cluster", "system_disk")), type_='cloud_efficiency')
        else:
            configs.add_system_disk(size=40, type_='cloud_efficiency')
        configs.InstanceCount = 1
        cluster.Configs = configs
        self.cluster = cluster

    def AddClusterMount(self, writeable=True):
        self.oss_mount_entry = {
            "Source": self.conf.rget("OSS", "ossSrc"),
            "Destination": self.conf.rget("OSS", "mountPath"),
            "WriteSupport": writeable,
        }
        self.cluster.Configs.Mounts.add_entry(self.oss_mount_entry)
        self.cluster.Configs.Mounts.Locale = "utf-8"
        self.cluster.Configs.Mounts.Lock = False


class Task(object):

    def __init__(self, cluster=None):
        self.job_desc = JobDescription()
        self.task_dag = DAG()
        self.stdout = cluster.conf.rget("Task", "stdoutlog")
        self.stderr = cluster.conf.rget("Task", "stderrlog")
        if not self.stdout.endswith("/"):
            self.stdout += "/"
        if not self.stderr.endswith("/"):
            self.stderr += "/"
        self.timeout = int(cluster.conf.rget("Task", "timeout"))
        self.cluster = cluster

    def AddOneTask(self, job=None, outdir=""):
        jobcpu = job.cpu if job.cpu else self.cluster.conf.rget("args", "cpu")
        jobmem = job.mem if job.mem else self.cluster.conf.rget(
            "args", "memory")
        insType = self.cluster.conf.rget("Cluster", "InstanceType")
        if insType is not None:
            if self.cluster.conf.it_conf[insType]["cpu"] < int(jobcpu) or self.cluster.conf.it_conf[insType]["memory"] < int(jobmem):
                insType = self.get_instance_type(int(jobcpu), int(jobmem))
                self.cluster.conf.logger.info("job %s %s InstanceType not match cpu or memory necessary, used %s insteaded",
                                              job.name, self.cluster.conf.rget("Cluster", "InstanceType"),  insType)
        else:
            insType = self.get_instance_type(int(jobcpu), int(jobmem))
        self.cluster.cluster.InstanceType = insType
        task = TaskDescription()
        task.Parameters.Command.CommandLine = "sh -c \"%s\"" % job.cmd
        task.Parameters.StdoutRedirectPath = self.stdout
        task.Parameters.StderrRedirectPath = self.stderr
        task.InstanceCount = 1
        task.Timeout = self.timeout
        task.MaxRetryCount = 0
        task.AutoCluster = self.cluster.cluster
        if self.cluster.conf.rget("Task", "DOCKER_IMAGE"):
            task.Parameters.Command.EnvVars["BATCH_COMPUTE_DOCKER_IMAGE"] = self.cluster.conf.rget(
                "Task", "DOCKER_IMAGE")
            task.Parameters.Command.EnvVars["BATCH_COMPUTE_DOCKER_REGISTRY_OSS_PATH"] = self.cluster.conf.rget(
                "Task", "DOCKER_REGISTRY_OSS_PATH")
            task.Mounts = self.cluster.cluster.Configs.Mounts
            self.cluster.cluster.Configs.Mounts = Mounts()
        if outdir is not None:
            if self.cluster.oss_mount_entry["Destination"] in outdir:
                if not outdir.endswith("/"):
                    outdir += "/"
                des = self.cluster.oss_mount_entry["Destination"]
                src = self.cluster.oss_mount_entry["Source"]
                task.OutputMapping[outdir] = re.sub("^"+des, src, outdir)
            else:
                self.cluster.conf.logger.debug(
                    "Only oss mount path support(%s), %s", outdir, job.name)
        self.task_dag.add_task(task_name=job.name, task=task)
        self.name = job.name
        self.client = self.cluster.client
        job.client = self.client

    def modifyTaskOutMapping(self, job=None, mapping=""):
        if mapping:
            if self.cluster.oss_mount_entry["Destination"] in mapping:
                # if not mapping.endswith("/"):
                # mapping += "/"
                des = self.cluster.oss_mount_entry["Destination"]
                src = self.cluster.oss_mount_entry["Source"]
                self.task_dag.Tasks[self.name].OutputMapping.clear()
                self.task_dag.Tasks[self.name].OutputMapping[mapping] = re.sub(
                    "^"+des, src, mapping)
            else:
                self.cluster.conf.logger.debug(
                    "Only oss mount path support(%s), %s", mapping, job.name)

    def get_instance_type(self, cpu=2, mem=4):
        e = []
        c, m = 0, 0
        for tp in self.cluster.conf.availableTypes:
            cc = self.cluster.conf.it_conf[tp]["cpu"]
            cm = self.cluster.conf.it_conf[tp]['memory']
            if cc >= cpu and cm >= mem:
                if len(e):
                    if cc != c or cm != m:
                        break
                c, m = cc, cm
                e.append(tp)
        if not e:
            return "ecs.c5.large"
        return sample(e, 1)[0]

    def Submit(self):
        self.job_desc.DAG = self.task_dag
        self.job_desc.Priority = 1
        self.job_desc.Name = self.name
        self.job_desc.Description = self.name
        self.job_desc.JobFailOnInstanceFail = True
        self.job_desc.AutoRelease = False
        clientjob = self.client.create_job(self.job_desc)
        self.id = clientjob.Id

    @property
    def loger(self):
        return context.log
