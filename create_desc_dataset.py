# coding: utf-8

from collections import defaultdict, Counter
from pprint import pprint
import common
from common import RED, RESET, BLUE, BOLD, UNDERLINE, GREEN
from common import dbgprint, separate_path_and_filename, recDotDefaultDict
import argparse, collections, re, os, time, sys, codecs, json, random, copy
random.seed(0)

@common.timewatch
def read_jsonlines(source_path, max_rows=0):
  data = {}
  for i, l in enumerate(open(source_path)):
    if max_rows and i >= max_rows:
      break
    d = json.loads(l)
    data.update({d['qid']:d})
  return common.recDotDict(data)

def divide_data(data, n_train, n_dev, n_test):
  assert n_dev + n_test <= len(data)
  if not n_train or n_train + n_dev + n_test <= len(data):
    n_train = len(data) - n_dev - n_test
  #print(len(data), n_train, n_dev, n_test)
  article_keys = set(data.keys())
  train_keys = random.sample(article_keys, n_train)
  article_keys -= set(train_keys)
  dev_keys = random.sample(article_keys, n_dev)
  article_keys -= set(dev_keys)
  test_keys = random.sample(article_keys, n_test)

  train = {k:data[k] for k in train_keys}
  dev = {k:data[k] for k in dev_keys}
  test = {k:data[k] for k in test_keys}
  return train, dev, test

def dump_as_json(entities, file_path, as_jsonlines=True):
  if as_jsonlines:
    if os.path.exists(file_path):
      os.system('rm %s' % file_path)
    with open(file_path, 'a') as f:
      for entity in entities.values():
        json.dump(entity, f, ensure_ascii=False)
        f.write('\n')
  else:
    with open(file_path, 'w') as f:
      json.dump(entities, f, indent=4, ensure_ascii=False)

def statistics(data):
  n_entities = len(data)
  n_descs = len([d.desc for d in data.values() if d.desc.strip()])
  n_sents = sum([sum([len(p) for p in d.text]) for d in data.values()])
  n_contexts = sum([len(d.contexts) for d in data.values()])
  if n_entities != n_descs:
    print('# of all entities:\t', n_entities)
  print('# of entities with description:\t', n_descs)
  #print('# of sentences in articles:\t%d (Avg = %f)' % (n_sents, n_sents/n_entities))
  print('# of contexts:\t%d (Avg = %f)' % (n_contexts, n_contexts/n_entities))


def color(sent, span):
  assert type(sent) == str
  s = sent.split()
  for i in range(span[0], span[1]+1):
    s[i] = BLUE + s[i] + RESET
  return ' '.join(s)
    
def print_example(data):
  for _, example in enumerate(data.values()):
    print('====== %s ======' % example.qid)
    print('<Wikipedia title>', example.title)
    print('<Wikidata name>', example.name)
    print('<Context>')
    for context, span in example.contexts[:5]:
      print(' - ' + color(context, span))
    print('<Desc>', example.desc.lower())
    print('')


def filter_desc(desc):
  if not desc:
    return False

  desc = desc.lower()
  if 'wikipedia' in desc or 'wikimedia' in desc:
    return False
  return True
  

@common.timewatch
def remove_unnecessary_data(data):
  new_data = recDotDefaultDict()
  for qid, d in data.items():
    d.contexts = [(sent, link) for sent, link in d.contexts if sent and link[1] >= link[0]]
    if filter_desc(d.desc) and len(d.contexts) > 0:
      new_data[qid].qid = d.qid
      new_data[qid].title = d.title
      new_data[qid].name = d.name
      new_data[qid].desc = d.desc
      new_data[qid].contexts = d.contexts
  return new_data
  

@common.timewatch
def main(args):
  source_dir, _ = separate_path_and_filename(args.source_path)
  target_path = source_dir + '/desc' if not args.target_path else args.target_path

  data = read_jsonlines(args.source_path, max_rows=args.max_rows)
  data = remove_unnecessary_data(data)
  statistics(data)
  # print_example(data)
  # exit(1)
  assert args.train_percentage >= 0 and args.train_percentage <= 100
  #train_percentage = args.train_percentage if args.train_percentage else 90
  train_percentage = args.train_percentage
  n_dev = n_test = int(len(data) * (100 - train_percentage) / 2 * 0.01)
  n_train = len(data) - n_dev - n_test
  train, dev, test = divide_data(data, n_train, n_dev, n_test)
  
  print('<train>')
  statistics(train)
  print('<valid>')
  statistics(dev)
  print('<test>')
  statistics(test)
  if not os.path.exists(target_path):
    os.makedirs(target_path)

  dump_as_json(train, os.path.join(target_path, 'train.jsonlines'))
  dump_as_json(dev, os.path.join(target_path, 'dev.jsonlines'))
  dump_as_json(test, os.path.join(target_path, 'test.jsonlines'))

  dump_as_json(train, os.path.join(target_path, 'train.json'), as_jsonlines=False)
  dump_as_json(dev, os.path.join(target_path, 'dev.json'), as_jsonlines=False)
  dump_as_json(test, os.path.join(target_path, 'test.json'), as_jsonlines=False)



if __name__ == "__main__":
  desc = 'The old version of create_desc_and_category_dataset.py'
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('-tp', '--train_percentage', type=int, default=90)

  parser.add_argument('-s', '--source_path',
                      default='wikiP2D/latest/merged.jsonlines', help='')
  parser.add_argument('-t', '--target_path', default=None)
  parser.add_argument('-mr', '--max_rows', type=int, default=0)
  args = parser.parse_args()
  main(args)
