#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, shutil, csv
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
import sframe as sf
from sklearn.decomposition import TruncatedSVD
from sklearn.externals import joblib
from infer import InferDoc
from tools.matrix import *
from contrib.corpus import FeaCorpus
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import luigi.contrib.ssh

class SVD(luigi.Task):
	conf = luigi.Parameter()
		
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.svd_model = '%s/data/train/svd.model/svd.model' % root
		self.sample_fraction = parser.getfloat('svd', 'sample_fraction')
		self.n_components = parser.getint('svd', 'n_components')
		self.sampled_doc = '%s/data/temp/paper.topic.sampled' % root
		self.topic_num = parser.getint('plda+', 'topic_num')
		

	def requires(self):
		return [InferDoc(self.conf)]

	def output(self):
		return luigi.LocalTarget(self.svd_model)
	
	def run(self):
		model_dir = os.path.dirname(self.svd_model)
		if os.path.exists(model_dir):
			shutil.rmtree(model_dir)
		os.mkdir(model_dir)

		df = sf.SFrame.read_csv(self.input()[0].fn,
			column_type_hints=[str, str],
			delimiter='\t', header=False)
		df = df.sample(self.sample_fraction)	
		df.export_csv(self.sampled_doc, delimiter="\t", quote_level=csv.QUOTE_NONE, header=False)

		fea_corpus = FeaCorpus(self.sampled_doc)	
		X = load_csr_matrix(fea_corpus, self.topic_num)
		model = TruncatedSVD(n_components=self.n_components)	
		model.fit(X)
		joblib.dump(model, self.output().fn)
		os.remove(self.sampled_doc)

if __name__ == "__main__":
    luigi.run()
