#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, csv, json
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)

from tools.vocab import Vocab, WordSet
from get_paper_data import GetPaper
from get_train_data import MakeTrainingDict
import sframe as sf
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
	
class Target2LDA(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.target_plda = '%s/data/target/paper.plda' % root

	def output(self):
		return luigi.LocalTarget(self.target_plda)

	def requires(self):
		return [GetPaper(self.conf), MakeTrainingDict(self.conf)]

	def run(self):
		df = sf.load_sframe(self.input()[0].fn)
		self.wordset = WordSet(self.input()[1].fn)
		df['ensemble'] = df.apply(self.ensemble)
		df = df.select_columns(['id', 'ensemble'])
		df = df[df['ensemble'].apply(lambda x: 1 if len(x) > 0 else 0)]
		df.export_csv(self.output().fn, quote_level=csv.QUOTE_NONE, delimiter='\t', header=False)
	
	def ensemble(self, row):
		arr = []
		b = self.wordset.filter_bows(row['union'])
		if b != '':
			arr.append({"b": b, "w": 1.0})
		return arr	

if __name__ == "__main__":
    luigi.run()	
