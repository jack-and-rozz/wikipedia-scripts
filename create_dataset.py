# coding: utf-8
from orderedset import OrderedSet
from pprint import pprint
from collections import OrderedDict, defaultdict, Counter
import argparse, sys, os, time, json, subprocess, re, itertools, random, math
from common import str2bool, timewatch, multi_process, flatten
import common

try:
   import pickle as pickle
except:
   import pickle


@timewatch
def load_or_create_chunks(target_dir, filename):
  output_path = os.path.join(target_dir, filename)
  if not os.path.exists(target_dir):
    os.makedirs(target_dir)

  chunk_size = 10000
  if args.cleanup or subprocess.getoutput('ls -d %s/* | grep "%s\.[0-9]\+"' % (target_dir, filename)).split()[0] == 'ls:':
    sys.stdout.write('Create new Wikipedia page chunks from %s to %s' % (os.path.join(args.source_dir, filename), output_path))
    data = pickle.load(open(os.path.join(args.source_dir, filename), 'rb'))
    for i, d in itertools.groupby(enumerate(data), lambda x: x[0] // chunk_size):
      chunk = OrderedDict([(x[1],data[x[1]]) for x in d])
      pickle.dump(chunk, open(output_path + '.%02d' %i, 'wb'))
  else:
    sys.stdout.write('Wikipedia page chunks are found at %s\n' % (target_dir))
    data = OrderedDict()
    chunk_files = subprocess.getoutput('ls -d %s/* | grep "%s\.[0-9]\+"' % (target_dir, filename)).split()
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
                   n_valid=2000, n_test=2000):
  ds = common.rec_dotdefaultdict()
  linked_subjects = set(pages.keys()).intersection(set(triples.keys()))
  svalid = set(random.sample(linked_subjects, n_valid))
  stest = set(random.sample(linked_subjects - svalid, n_test))
  strain = linked_subjects - svalid - stest
  # for k in stest:
  #   print k, subjects[k]
  # exit(1)
  ds.articles.train = {k:pages[k] for k in strain}
  ds.triples.train = {k:triples[k] for k in strain}
  ds.subjects.train = {k:subjects[k] for k in strain}

  ds.articles.valid = {k:pages[k] for k in svalid}
  ds.triples.valid = {k:triples[k] for k in svalid}
  ds.subjects.valid = {k:subjects[k] for k in svalid}

  ds.articles.test = {k:pages[k] for k in stest}
  ds.triples.test = {k:triples[k] for k in stest}
  ds.subjects.test = {k:subjects[k] for k in stest}
  
  ds.relations = relations
  ds.objects = objects
  return ds

@timewatch
def print_stat(ds):
  
  for k in ['train', 'valid', 'test']:
    stat = (len(ds['subjects'][k]),  sum([len(a) for a in list(ds['articles'][k].values())]), sum([len(t) for t in list(ds['triples'][k].values())]))
    n_subjects, n_articles, n_triples = stat
    sys.stdout.write('(%s): (articles, triples, subjects) = %d %d %d\n' % (k, n_articles, n_triples, n_subjects))
  n_relations, n_objects = len(ds['relations']), len(ds['objects'])
  sys.stdout.write('(relations, objects) = %d %d \n' % (n_relations, n_objects))


def to_geneal_dump(dataset, target_dir, target_file):
  target_path = target_dir + target_file
  ds = {
    'relations': dataset.relations,
    'objects': dataset.objects,
    'subjects': dict(dataset.subjects),
    'triples': dict(dataset.triples),
    'articles': dict(dataset.articles),
  }
  
  pickle.dump(ds, open(target_path, 'wb'))

  # #with open(target_dir + 'relations.train.txt','w'):
  # for k in dataset.articles.train:
  #   pprint(dataset.articles.train[k])
  #   break
  # for r in dataset.relations:
  #   pprint(dataset.relations[r])
  #   break
  # for o in dataset.objects:
  #   pprint(dataset.objects[o])
  #   break
    

  return ds


@timewatch
def main(args):
  
  target_dir = os.path.join(args.source_dir, 'dataset/source')
  target_file = '/Q%dO%dR%d' % (args.min_qfreq, args.n_objects, args.n_relations)
  if args.target_suffix:
    target_file += ".%s.bin" % args.target_suffix
  else:
    target_file += ".bin" 

  random.seed(0)
  if not os.path.exists(target_dir + target_file):
    suffix = ".minq%d.bin" % (args.min_qfreq)
    pages = load_or_create_chunks(args.source_dir + '/chunk',  'pages' + suffix)
    suffix = ".minq%d.o%dr%d.bin" % (args.min_qfreq, args.n_objects, args.n_relations)
    subjects = pickle.load(open(args.source_dir + '/subjects' + suffix, 'rb'))
    relations = pickle.load(open(args.source_dir + '/relations' + suffix, 'rb'))
    objects = pickle.load(open(args.source_dir + '/objects' + suffix, 'rb'))
    triples = pickle.load(open(args.source_dir + '/triples' + suffix, 'rb'))
    if not os.path.exists(target_dir):
      os.makedirs(target_dir)

    dataset = create_dataset(pages, subjects, relations, objects, triples)
    #pickle.dump(dataset, open(target_dir + target_file, 'wb'))
    ds = to_geneal_dump(dataset, target_dir, target_file)

  else:
    dataset = pickle.load(open(target_dir + target_file, 'rb'))
  print_stat(dataset)
  return



if __name__ == "__main__":
  desc = "This script divides processed Wikipedia pages and Wikidata Items, Triples into train, valid, test data."
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', default='wikiP2D.p1s1')
  parser.add_argument('--target_suffix', default='')
  
  parser.add_argument('--min_qfreq', default=5, type=int)
  parser.add_argument('--n_objects', default=15000, type=int)
  parser.add_argument('--n_relations', default=300, type=int)
  parser.add_argument('--cleanup', default=False, type=str2bool)
  parser.add_argument('--n_files', default=0, type=int)
  parser.add_argument('--n_processes', default=4, type=int)

  # optional 
  args = parser.parse_args()
  main(args)
