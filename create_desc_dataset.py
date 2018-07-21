# coding: utf-8

from collections import defaultdict, Counter
from pprint import pprint
import common
from common import RED, RESET, BLUE, BOLD, UNDERLINE, GREEN
import argparse, collections, re, os, time, sys, codecs, json, random, copy
random.seed(0)

def read_jsonlines(source_path, max_rows=0):
  data = {}
  for i, l in enumerate(open(source_path)):
    if max_rows and i >= max_rows:
      break
    d = json.loads(l)
    data.update({d['qid']:d})
  return common.recDotDict(data)

@common.timewatch
def extract_links(data):
  contexts_by_qid = collections.defaultdict(list)
  for subj_qid, v in data.items():
    for link_qid, position in v.link.items():
      para, sent, span = position
      contexts_by_qid[link_qid].append((v.text[para][sent], span))
  return contexts_by_qid

@common.timewatch
def merge_contexts(data, contexts, n_max_context):
  new_data = common.recDotDict()
  for qid in data:
    if data[qid].desc and contexts[qid]:
      new_data[qid] = data[qid]
      if len(contexts[qid]) > n_max_context:
        new_data[qid].context = random.sample(contexts[qid], n_max_context)
      else:
        new_data[qid].context = contexts[qid]
  return new_data

def divide_data(data, n_train, n_dev, n_test):
  assert n_dev + n_test <= len(data)
  if not n_train or n_train + n_dev + n_test <= len(data):
    n_train = len(data) - n_dev - n_test
  print(len(data), n_train, n_dev, n_test)
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
  context_cnt = defaultdict(int)
  entity_cnt = defaultdict(int)
  for a_qid, d in data.items():
    context_cnt[a_qid] += len(d.context)

  def _sort(counter):
    return sorted(counter.items(), key=lambda x:-x[1])

  data = collections.OrderedDict([(qid, data[qid]) for qid, _ in _sort(context_cnt)])
  context_cnt = _sort(context_cnt)
  sys.stdout = sys.stderr
  print('# of articles: ', len(data))
  print('# of contexts: ', sum([v for k, v in context_cnt]))
  print() 
  sys.stdout = sys.__stdout__

def histgram(data, target_path):
  import matplotlib.pyplot as plt
  fig = plt.figure()
  ax = fig.add_subplot(1,1,1)
  x = [len(data[qid].context) for qid in data]
  ax.hist(x, bins=50)
  ax.set_title('# of contexts for an entiry ')
  ax.set_xlabel('x')
  ax.set_ylabel('freq')
  fig.savefig(target_path)

@common.timewatch
def main(args):
  target_dir = args.target_dir if args.target_dir else args.source_dir + '/desc'
  data = read_jsonlines(os.path.join(args.source_dir, args.filename), 
                        max_rows=args.max_rows)
  contexts_by_qid = extract_links(data)
  data = merge_contexts(data, contexts_by_qid, args.n_max_context)

  statistics(data)
  histgram(data, target_dir + '/contexts_histgram.png')

  train, dev, test = divide_data(data, args.n_train, args.n_dev, args.n_test)

  if not os.path.exists(target_dir):
    os.makedirs(target_dir)

  dump_as_json(train, os.path.join(target_dir, 'train.jsonlines'))
  dump_as_json(dev, os.path.join(target_dir, 'dev.jsonlines'))
  dump_as_json(test, os.path.join(target_dir, 'test.jsonlines'))

  dump_as_json(train, os.path.join(target_dir, 'train.json'), as_jsonlines=False)
  dump_as_json(dev, os.path.join(target_dir, 'dev.json'), as_jsonlines=False)
  dump_as_json(test, os.path.join(target_dir, 'test.json'), as_jsonlines=False)



if __name__ == "__main__":
  desc = 'The old version of create_desc_and_category_dataset.py'
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--n_train', type=int, default=0)
  parser.add_argument('--n_dev', type=int, default=1000)
  parser.add_argument('--n_test', type=int, default=1000)

  parser.add_argument('-s', '--source_dir',
                      default='wikiP2D/wikiP2D.p1s0',
                      help='')
  parser.add_argument('-t', '--target_dir', default='')
  parser.add_argument('-f', '--filename', default='merged.jsonlines')
  parser.add_argument('-mr', '--max_rows', type=int, default=0)
  parser.add_argument('-ml', '--min_n_links', type=int, default=3)
  parser.add_argument('-nr', '--n_relations', type=int, default=400)

  parser.add_argument('--n_max_context', type=int, default=10)

  parser.add_argument('--debug', type=common.str2bool, default=False)
  args = parser.parse_args()
  main(args)
