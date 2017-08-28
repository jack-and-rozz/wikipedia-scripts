# coding: utf-8
from orderedset import OrderedSet
from pprint import pprint
from collections import OrderedDict, defaultdict, Counter
import argparse, sys, os, time, json, commands, re, itertools, random, math
from common import str2bool, timewatch, multi_process, flatten
import common

try:
   import cPickle as pickle
except:
   import pickle

def print_triples_by_name(element, idx, subjects, relations, objects, triples):
  tmp = flatten([[(s,r,o) for (r,o) in v] for s,v in triples.items()])
  selected = [t for t in tmp if t[idx] == element]

  
  print len(subjects), len(relations), len(objects), len(o)
  print len(set([t[0] for t in tmp])), len(set([t[1] for t in tmp])), len(set([t[2] for t in tmp]))
  exit(1)
  for s,r,o in selected:
    if s in subjects and r in relations and o in objects:
      print s,r,o,
      print '\t',
      print (subjects[s]['name'], relations[r]['name'], objects[o]['name']) 
  pprint(subjects[s])
  
@timewatch
def load_or_create_chunks(target_dir, filename):
  output_path = os.path.join(target_dir, filename)
  if not os.path.exists(target_dir):
    os.makedirs(target_dir)

  chunk_size = 10000
  if args.cleanup or not commands.getoutput('ls -d %s/* | grep "%s\.[0-9]\+"' % (target_dir, filename)).split() and False:
    data = pickle.load(open(os.path.join(args.source_dir, filename), 'rb'))
    for i, d in itertools.groupby(enumerate(data), lambda x: x[0] // chunk_size):
      chunk = OrderedDict([(x[1],data[x[1]]) for x in d])
      pickle.dump(chunk, open(output_path + '.%02d' %i, 'wb'))
  else:
    data = OrderedDict()
    chunk_files = commands.getoutput('ls -d %s/* | grep "%s\.[0-9]\+"' % (target_dir, filename)).split()
    if args.n_files:
      chunk_files.reverse()
      chunk_files = chunk_files[:args.n_files]
      chunk_files.reverse()

    def load(pathes_per_process):
      res = OrderedDict()
      dumps = [pickle.load(open(p, 'rb')) for p in pathes_per_process]
      for dump in dumps:
        res.update(dump)
      return res

    n_processes = min(args.n_processes, len(chunk_files))
    chunk_size = math.ceil((1.0 * len(chunk_files) / n_processes))
    pathes = [[x[1] for x in p] for i, p in itertools.groupby(enumerate(chunk_files), lambda x: x[0] // chunk_size)]
    data_per_process = multi_process(load, pathes)
    data = OrderedDict()
    for d in data_per_process:
      data.update(d)
  return data

def create_dataset(pages, subjects, relations, objects, triples, 
                   n_valid=500, n_test=500):
  dataset = recdotdefaultdict()
  
  

  linked_subjects = set(pages.keys()).intersection(set(triples.keys()))
  valid_subjects = set(random.sample(linked_subjects, n_valid))
  test_subjects = set(random.sample(linked_subjects - valid_subjects, n_test))
  train_subjects = linked_subjects - valid_subjects - test_subjects
  print len(train_subjects), len(valid_subjects), len(test_subjects)
  #print len(subjects), len(relations), len(objects), len(o)
  #print len(set([t[0] for t in tmp])), len(set([t[1] for t in tmp])), len(set([t[2] for t in tmp]))
  exit(1)

  #data = defaultdict(dict)
  


@timewatch
def main(args):
  random.seed(0)
  #dataset = recdotdefaultdict()
  import collections
  print collections.__file__
  dataset = common.rec_dotdefaultdict(dict)
  exit(1)

  suffix = ".minq%d.bin" % (args.min_qfreq)
  pages = load_or_create_chunks(args.source_dir + '/chunk',  'pages' + suffix)
  subjects = pickle.load(open(args.source_dir + '/subjects' + suffix, 'rb'))
  suffix = ".minq%d.o%dr%d.bin" % (args.min_qfreq, args.n_objects, args.n_relations)
  relations = pickle.load(open(args.source_dir + '/relations' + suffix, 'rb'))
  objects = pickle.load(open(args.source_dir + '/objects' + suffix, 'rb'))
  triples = pickle.load(open(args.source_dir + '/triples' + suffix, 'rb'))

  if not os.path.exists(args.target_dir):
    os.makedirs(args.target_dir)
  create_dataset(pages, subjects, relations, objects, triples)
  return
  # for i,(k,v) in enumerate(pages.items()):
  #   pprint(pages[k])
  #   if i == 1000:
  #     return
    
  # print len(pages)
  # return
  # pprint(dict(subjects))
  # #pprint(dict(pages))
  # exit(1)
  #print subjects.keys()[:10]
  #print objects.keys()[:10]

  for key in subjects.keys()[60:900]:
    print_triples_by_name(key, 0,
                          subjects, relations, objects, triples)
  
  return
  for k,v in relations.items():
    print k
    pprint(v)
  #pprint(dict(relations))

if __name__ == "__main__":
  desc = ""
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', default='wikiP2D')
  parser.add_argument('--target_dir', default='wikiP2D/dataset/processed')
  parser.add_argument('--min_qfreq', default=5, type=int)
  parser.add_argument('--n_objects', default=15000, type=int)
  parser.add_argument('--n_relations', default=300, type=int)
  parser.add_argument('--cleanup', default=False, type=str2bool)
  parser.add_argument('--n_files', default=4, type=int)
  parser.add_argument('--n_processes', default=4, type=int)

  # optional 
  args = parser.parse_args()
  main(args)
