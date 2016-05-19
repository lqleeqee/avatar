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
from plda import PLDA
from prepare.get_target_data import Target2LDA
from tools.inferer import infer_topic
from tools.mysql.import_doc_topic import import_doc_topic

class InferDoc(luigi.Task):
	conf = luigi.Parameter()
		
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")	
		self.infer_topic = '%s/data/target/paper.topic' % root

	def requires(self):
		plda_target_task = Target2LDA(self.conf) 
		plda_model_task = PLDA(self.conf)
		self.plda_target = plda_target_task.output()
		self.plda_model_target = plda_model_task.output()
		return [plda_target_task, plda_model_task]
        
	def output(self):
                return luigi.LocalTarget(self.infer_topic)

	def run(self):
		infer_topic(self.plda_target.fn, self.plda_model_target.fn, self.output().fn, self.conf)

class Doc2Mysql(luigi.Task):
	conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                self.host = parser.get("mysql", "host")
                self.db = parser.get("mysql", "db")
                self.user = parser.get("mysql", "user")
                self.passwd = parser.get("mysql", "password")

        def requires(self):
                return [InferDoc(self.conf)]

        def output(self):
                return None
		
	def run(self):
		import_doc_topic(self.input()[0].fn, self.host, self.db, self.user, self.passwd)	

if __name__ == "__main__":
    luigi.run()
