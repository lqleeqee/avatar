#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import luigi.contrib.ssh

import socket
from prepare.get_train_data import Training2LDA
from tools.mysql.import_model_view import import_model_view
from tools.view_model import plda_model_view

class PLDA(luigi.Task):
	conf = luigi.Parameter()
		
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.ssh_user = parser.get("basic", "ssh_user")
		self.ssh_port = parser.get('basic', 'ssh_port')
		self.topic_num = parser.getint('plda+', 'topic_num')
		self.burn_in_iter = parser.getint('plda+', 'plda_burn_in_iter')
		self.total_iter = parser.getint('plda+', 'plda_total_iter')
		self.cpu_core_num = parser.getint('plda+', 'cpu_core_num')		
		self.alpha = 50.0 / self.topic_num
		self.mf = '%s/data/temp/mf' % root
		self.mpi_plda = '%s/plda/mpi_lda' % root
		self.plda_model = '%s/data/train/plda.model.txt' % root
		self.plda_model_tmp = self.plda_model + ".tmp"

	def requires(self):
		return [Training2LDA(self.conf)]

	def output(self):
		return luigi.LocalTarget(self.plda_model)
	
	def run(self):
		mpi_nodes = [node.strip() for node in os.popen('mpdtrace').readlines()]
		if len(mpi_nodes) == 0:
			return
		job_num = len(mpi_nodes) * self.cpu_core_num
		with open(self.mf, 'w') as mf_fd:
			for node in mpi_nodes:
				print >> mf_fd, "%s:%d" % (node, self.cpu_core_num)
		localhostname = socket.gethostname()
		localhostname_ = '.'.join(localhostname.split('.')[0:-1])
		for node in mpi_nodes:
			if node != localhostname and node != localhostname_:
				rfs = luigi.contrib.ssh.RemoteFileSystem(node, port=self.ssh_port, username=self.ssh_user)
				print "sending %s to %s" % (self.input()[0].fn, node)
				rfs.put(self.input()[0].fn, self.input()[0].fn)
		cmd = '''
			mpiexec -machinefile %s -n %d \
                		%s \
                		--num_topics %d --alpha %f --beta 0.01 \
                		--training_data_file %s \
                		--model_file %s \
                 		--total_iterations %d
		'''
		cmd = cmd % (self.mf, job_num, self.mpi_plda, 
				self.topic_num, self.alpha, self.input()[0].fn, 
				self.plda_model_tmp, self.total_iter)
		os.system(cmd)
		if os.path.exists(self.mf):
			os.remove(self.mf)
		if os.path.exists(self.plda_model_tmp):
			os.rename(self.plda_model_tmp, self.output().fn)

class PLDA2Mysql(luigi.Task):
	conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
		root = parser.get("basic", "root")	
		self.model_view = "%s/data/temp/plda.model.view" % root
                self.host = parser.get("mysql", "host")
                self.db = parser.get("mysql", "db")
                self.user = parser.get("mysql", "user")
                self.passwd = parser.get("mysql", "password")

        def requires(self):
                return [PLDA(self.conf)]

        def output(self):
                return None
		
	def run(self):
		plda_model_view(self.input()[0].fn, self.model_view)
		import_model_view(self.model_view, self.host, self.db, self.user, self.passwd)
		os.remove(self.model_view)
		
if __name__ == "__main__":
    luigi.run()
