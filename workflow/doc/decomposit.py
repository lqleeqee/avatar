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

from contrib.corpus import FeaCorpus
from sklearn.externals import joblib
from infer import InferDoc
from svd import SVD
from tools.matrix import *

class DecompositDoc(luigi.Task):
	conf = luigi.Parameter()
		
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.topic_num = parser.getint("plda+", "topic_num")
		self.doc_decompisit = '%s/data/target/paper.topic.decompisited' % root

	def requires(self):
		return [InferDoc(self.conf), SVD(self.conf)]	

	def output(self):
		return luigi.LocalTarget(self.doc_decompisit) 

	def run(self):
		model = joblib.load(self.input()[1].fn)
		fea_corpus = FeaCorpus(self.input()[0].fn)
		ids = [id for id in FeaCorpus(self.input()[0].fn, onlyID=True)]
		X = load_csr_matrix(fea_corpus, self.topic_num)
		Y = model.transform(X)
		with self.output().open('w') as out_fd:
			print_dense_matrix(out_fd, ids, Y)	
		
if __name__ == "__main__":
    luigi.run()
