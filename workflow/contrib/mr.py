#!/usr/bin.python
# -*- coding: utf-8 -*-
import os
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import commands

def cmd(cmdstr):
        #执行Liunx Shell命令并返回状态码
        #cmmands:Python中的commands模块专门用于调用Linux shell命令，并返回状态和结果。
	exit_code = commands.getstatusoutput(cmdstr)[0]
        #执行shell命令, 返回两个元素的元组tuple(status, result)，
        #status为int类型，result为string类型，返回结果包含标准输出和标准错误。
	return exit_code

def check_mr_success(hdfs_dir):
        #create_hadoopcli_client ：
        #Given that we want one of the hadoop cli clients (unlike snakebite), this one will return the right one.
        hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
        #Use hadoop fs -stat to check file existence.
        return hdfs.exists('%s/_SUCCESS' % hdfs_dir)

def mr_cmd(javabin, jobname):
	javapath = os.path.dirname(javabin)
	jarbin = os.path.basename(javabin)
        cmd_str = 'cd %s && rm -rf status && java -jar %s -jobStreamName=%s -dataAnalysisName=%s && cd -'
        cmd_str = cmd_str % (javapath, jarbin, jobname, jobname)
        exit_code = cmd(cmd_str)
        return exit_code

def get_mr_dir(mr_dir, local_fd):
	if not check_mr_success(mr_dir):
		raise Exception('mr dir[%s] has no SUCESS flag' % mr_dir)
	src = luigi.contrib.hdfs.HdfsTarget('%s/part-*' % mr_dir)		
	with src.open() as in_fd:
		for line in in_fd:
			local_fd.write(line)
