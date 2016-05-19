// Copyright 2008 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*
  An example running of this program:

  ./infer \
  --alpha 0.1    \
  --beta 0.01                                           \
  --inference_data_file ./testdata/test_data.txt \
  --inference_result_file /tmp/inference_result.txt \
  --model_file /tmp/lda_model.txt                       \
  --burn_in_iterations 10                              \
  --total_iterations 15
*/
#include <iostream>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
//#include <algorithm>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/rapidjson.h>
#include "common.h"
#include "document.h"
#include "model.h"
#include "sampler.h"
#include "cmd_flags.h"

using learning_lda::LDACorpus;
using learning_lda::LDAModel;
using learning_lda::LDAAccumulativeModel;
using learning_lda::LDASampler;
using learning_lda::LDADocument;
using learning_lda::LDACmdLineFlags;
using learning_lda::DocumentWordTopicsPB;
using learning_lda::RandInt;
using rapidjson::Document;
using rapidjson::Value;
using rapidjson::kParseDefaultFlags;
using std::ifstream;
using std::ofstream;
using std::istringstream;
using std::cout;

static void initDocumentTopic(istringstream& ss,
                  const map<string, int>& word_index_map,
                  int num_topics,
                  DocumentWordTopicsPB& document_topics);
static void sampleDocument(LDADocument& document,
                    LDASampler& sampler,
                    const LDACmdLineFlags& flags,
                    TopicProbDistribution& prob_dist);
static void writeProbDist(const string& id, 
                  const TopicProbDistribution& prob_dist,
                  const LDACmdLineFlags& flags);
void smooth(TopicProbDistribution& prob_dist, 
            const LDACmdLineFlags& flags);
static void normalize(TopicProbDistribution& prob_dist);
static void weight(TopicProbDistribution& prob_dist, float w);
static void sum_up(TopicProbDistribution& prob_dist_a, 
            const TopicProbDistribution& prob_dist_b);

int main(int argc, char** argv) {
  LDACmdLineFlags flags;
  flags.ParseCmdFlags(argc, argv);
  if (!flags.CheckMRInferringValidity()) {
    return -1;
  }
  srand(time(NULL));
  map<string, int> word_index_map;
  ifstream model_fin(flags.model_file_.c_str());
  LDAModel model(model_fin, &word_index_map);
  LDASampler sampler(flags.alpha_, flags.beta_, &model, NULL);
  string line;
  while (getline(std::cin, line)) {  // Each line is a training document.
    if (line.size() > 0 &&      // Skip empty lines.
        line[0] != '\r' &&      // Skip empty lines.
        line[0] != '\n' &&      // Skip empty lines.
        line[0] != '#') {       // Skip comment lines.
      std::size_t pos = line.find_first_of('\t');
      if(pos == string::npos)
      {
        continue;
      }
      string id = line.substr(0, pos);
      string json = line.substr(pos + 1);
      //json.erase(std::remove(json.begin(), json.end(), '\\'), json.end());
      Document docs; docs.Parse(json.c_str());
      if (docs.HasParseError() || !docs.IsArray())
      {
        continue;
      }
      TopicProbDistribution doc_prob_dist(model.num_topics(), 0);
      for(int i = 0; i < docs.Size(); i++)
      {
        Value& doc = docs[i];
        float w = doc["w"].GetDouble();
        string bow = doc["b"].GetString();
        istringstream ss(bow);
	//init word-topic-table
	DocumentWordTopicsPB document_topics;
	initDocumentTopic(ss, word_index_map, model.num_topics(), document_topics);
	//init document
	LDADocument document(document_topics, model.num_topics());
	//infer one document
	TopicProbDistribution prob_dist(model.num_topics(), 0);
	sampleDocument(document, sampler, flags, prob_dist); 
        //smooth prob dist
        smooth(prob_dist, flags);
        //normalize and weight
	normalize(prob_dist);
        weight(prob_dist, w); 
        sum_up(doc_prob_dist, prob_dist);
      }
      normalize(doc_prob_dist);
      writeProbDist(id, doc_prob_dist, flags);  
    }
  }
}

void initDocumentTopic(istringstream& ss, 
                  const map<string, int>& word_index_map,
                  int num_topics, 
		  DocumentWordTopicsPB& document_topics)
{
  string word;
  int count;
  while (ss >> word >> count) {  // Load and init a document.
    vector<int32> topics;
    for (int i = 0; i < count; ++i) {
      topics.push_back(RandInt(num_topics));
    }
    map<string, int>::const_iterator iter = word_index_map.find(word);
    if (iter != word_index_map.end()) {
      document_topics.add_wordtopics(word, iter->second, topics);
    }
  }
}

void sampleDocument(LDADocument& document, 
		    LDASampler& sampler,
                    const LDACmdLineFlags& flags, 
		    TopicProbDistribution& prob_dist)
{
  for (int iter = 0; iter < flags.total_iterations_; ++iter) {
    sampler.SampleNewTopicsForDocument(&document, false);
    if (iter >= flags.burn_in_iterations_) {
      const vector<int64>& document_distribution =
        document.topic_distribution();
      for (int i = 0; i < document_distribution.size(); ++i) {
        prob_dist[i] += document_distribution[i];
      }
    }
  }
}
   
void writeProbDist(const string & id, 
                   const TopicProbDistribution& prob_dist,
                   const LDACmdLineFlags& flags) 
{
  bool sparse = (flags.sparse_ == "true");
  bool first = true;
  cout << id << "\t";
  for (int topic = 0; topic < prob_dist.size(); ++topic) {
    if(!sparse) {
      if(!first) cout << " ";
        first = false;
        cout << prob_dist[topic];
    } else if(prob_dist[topic] != 0) {
      if(!first) cout << " ";
        first = false;
        cout << topic << ":"
            << prob_dist[topic];
    }
  }
  cout << "\n";
}

void smooth(TopicProbDistribution& prob_dist, 
            const LDACmdLineFlags& flags)
{
  for (int topic = 0; topic < prob_dist.size(); ++topic)
  {
    prob_dist[topic] /= (flags.total_iterations_ - flags.burn_in_iterations_);
  } 
}

void normalize(TopicProbDistribution& prob_dist)
{
  float sum = 0.0;
  for (int topic = 0; topic < prob_dist.size(); ++topic)
  {
    sum += prob_dist[topic];
  }
  for (int topic = 0; topic < prob_dist.size(); ++topic)
  {
    prob_dist[topic] /= sum;
  }
}

void weight(TopicProbDistribution& prob_dist, float w)
{
  for (int topic = 0; topic < prob_dist.size(); ++topic)
  {
    prob_dist[topic] *= w;
  }
}

void sum_up(TopicProbDistribution& prob_dist_a, 
            const TopicProbDistribution& prob_dist_b)
{
  for (int topic = 0; topic < prob_dist_a.size(); ++topic)
  {
    prob_dist_a[topic] += prob_dist_b[topic];   
  }
}
