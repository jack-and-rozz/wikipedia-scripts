# coding: utf-8

from collections import defaultdict, Counter
from pprint import pprint
import common
from common import RED, RESET, BLUE, BOLD, UNDERLINE, GREEN
import argparse, collections, re, os, time, sys, codecs, json, random, copy
random.seed(0)

import nltk
from nltk.stem import WordNetLemmatizer 
from nltk.tree import ParentedTree
from nltk.parse.stanford import StanfordDependencyParser, StanfordParser


@common.timewatch
def get_category_noun(descriptions, debug=False):
  lemmatizer = WordNetLemmatizer()
  parser=StanfordParser(
  #model_path="edu/stanford/nlp/models/lexparser/englishFactored.ser.gz")
    model_path="edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz")

  assert(type(descriptions)) == list
  assert(type(descriptions[0])) == str
  def _get_head_noun(res):
    '''
    Return:
    - A string. The last noun in the first NP in a sentence. 
    However, there are several exceptions. 
     * "of" follows the abstract noun defined below (e.g. 'kind of musician')
     * "'s" follows the noun (e.g. "children 's book author")
    '''
    '''
    <memo>
    head-nounの抽出対象を他のNP句を含まないNP, とすると
    "((Children 's) book author)" -> 'child' となってしまう 
    (book authorはNPじゃないらしい)
    一方でそれを除くと
  'protein-coding gene in the species Homo sapiens' -> homo となってしまう
    (NPの中にPPなどがあった場合そのPPの名詞を取ってしまう)
    前者のほうが自然かなあ？
    '''
    abstract_words = set([
      'kind', 'sort', 'one', 'type', 'name',  # [Kazama+, EMNLP'07]
      'set', 'series', 'specie', 'subspecie', 'superfamily', 'family', 
      'genus', 'component', 'subclass', 'class', 'core', 'title', 'form', 
      'suborder', 'infraorder', 'order', 'head', 'sequence'])

    for tree in res: # なんでresをiterateする必要が？
      if len(tree.leaves()) == 1:
        return tree.leaves()[0], tree

      tree = ParentedTree.fromstring(str(tree))
      if debug:
        print(tree)
      for st in tree.subtrees():
        children_pos = [node.label() for node in st.subtrees() if node != st]
        if st.label() == 'NP' and 'NP' not in children_pos: 
        #if st.label() == 'NP':
          right_sibling = st.right_sibling()
          nns = [node for node in st.subtrees(
            filter=lambda x: x.label() in set(['NN', 'NNS']))]
          next_word = right_sibling.leaves()[0] if right_sibling else ''
          if len(nns) == 0:
            return '-', tree # Tagging failure
          else:
            # If "'s" follows the NP, move to the next NP. 
            if next_word == "'s":
              continue
            for i in reversed(range(len(nns))):
              last_word = lemmatizer.lemmatize(nns[i].leaves()[0]).lower() 
              if not (last_word in abstract_words and next_word == 'of'):
                return last_word, tree
    return '-', tree

  parse_results = parser.raw_parse_sents(descriptions)
  category_nouns = []
  for i, res in enumerate(parse_results):
    word, tree = _get_head_noun(res)
    category_nouns.append(word)
  return category_nouns

@common.timewatch
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
def merge_context(data, contexts, n_max_context):
  new_data = common.recDotDict()
  for qid in data:
    if contexts[qid]:
      # Using instance_of relation as category results in a skewed distribution...
      # categories = []
      # for s, r, o in data[qid].triples:
      #   if s == qid and r == 'P31' and o in data: # P31: instance_of
      #     categories.append(data[o].title)
      # if len(categories) > 1:
      #   pass
      #   #print(qid, data[qid].title)
      #   #print(categories)
      # if not (data[qid].desc or categories):
      #   continue
      if not data[qid].desc:
        continue
      new_data[qid] = data[qid]
      if len(contexts[qid]) > n_max_context:
        new_data[qid].contexts = random.sample(contexts[qid], n_max_context)
      else:
        new_data[qid].contexts = contexts[qid]
  return new_data

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
    context_cnt[a_qid] += len(d.contexts)

  def _sort(counter):
    return sorted(counter.items(), key=lambda x:-x[1])

  category_cnt = collections.Counter([d.category for d in data.values()])

  data = collections.OrderedDict([(qid, data[qid]) for qid, _ in _sort(context_cnt)])
  context_cnt = _sort(context_cnt)
  sys.stdout = sys.stderr
  print('# of articles: ', len(data))
  print('# of contexts: ', sum([v for k, v in context_cnt]))
  print('# of categories: ', (len(category_cnt)))
  sys.stdout = sys.__stdout__

def histgram(x, target_path, title=''):
  import matplotlib.pyplot as plt
  fig = plt.figure()
  ax = fig.add_subplot(1,1,1)
  ax.hist(x, bins=50)
  ax.set_title(title)
  ax.set_xlabel('x')
  ax.set_ylabel('freq')
  fig.savefig(target_path)

@common.timewatch
def get_category(data):
  categories = {}
  qids, descs = list(zip(*[(d.qid, d.desc) for d in data.values()]))
  descs = [desc if desc else '-' for desc in descs]
  category_nouns = get_category_noun(descs)
  for qid, category in zip(qids, category_nouns):
    categories[qid] = category
  return categories

# データ全体からランダムサンプル
@common.timewatch
def random_sample(data, n_train, n_dev, n_test):
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


@common.timewatch
def uniform_sample_for_category(data, n_train, n_dev, n_test):
  assert n_dev + n_test <= len(data)
  if not n_train or n_train + n_dev + n_test > len(data):
    n_train = len(data) - n_dev - n_test

  qids_by_category = defaultdict(list)
  for qid, d in data.items():
    qids_by_category[d.category].append(qid)

  dev_keys = set()
  def _sample(qids_by_category, N):
    keys = set()
    for i in range(N):
      category = random.choice(list(qids_by_category.keys()))
      qid = random.choice(qids_by_category[category])
      qids_by_category[category].remove(qid)
      if not qids_by_category[category]:
        del qids_by_category[category]
      keys.add(qid)
    return keys

  dev_keys = _sample(qids_by_category, n_dev)
  test_keys = _sample(qids_by_category, n_test)
  train_keys = _sample(qids_by_category, n_train)
  train = {k:data[k] for k in train_keys}
  dev = {k:data[k] for k in dev_keys}
  test = {k:data[k] for k in test_keys}
  return train, dev, test

@common.timewatch
def merge_category(data, categories, category_size):
  '''
  An example with a minor category (under top-N) will be removed.
  '''

  major_categories = sorted([(k, v) for k, v in Counter(categories.values()).items() if k != '-'], key=lambda x: -x[1])[:category_size]
  major_categories = set([k for (k, v) in major_categories])
  new_data = common.recDotDict()
  for qid in categories:
    #print(categories[qid])
    if categories[qid] in major_categories:
      new_data[qid] = data[qid]
      new_data[qid].category = categories[qid]
  return new_data

@common.timewatch
def main(args):
  target_dir = args.target_dir if args.target_dir else args.source_dir + '/desc_and_category'
  if not os.path.exists(target_dir):
    os.makedirs(target_dir)
  data = read_jsonlines(os.path.join(args.source_dir, args.filename), 
                        max_rows=args.max_rows)

  contexts_by_qid = extract_links(data)
  data = merge_context(data, contexts_by_qid, args.n_max_context)

  # Merge category info.
  categories = get_category(data)
  data = merge_category(data, categories, args.category_size)

  statistics(data)
  # Visualize
  title = 'Number of contexts histgram per an entiry'
  histgram([len(data[qid].contexts) for qid in data], 
           target_dir + '/contexts_histgram.png', title)


  if args.uniform_sample:
    train, dev, test = uniform_sample_for_category(data, args.n_train, args.n_dev, args.n_test)
  else:
    train, dev, test = random_sample(data, args.n_train, args.n_dev, args.n_test)


  # Plot frequencies of category labels for train, dev, test.
  category_freq = collections.Counter([d.category if d.category else '-' for d in train.values()])
  with open(target_dir + '/category_freq.train.txt', 'w') as f:
    sys.stdout = f
    for k, v in sorted(category_freq.items(), key=lambda x:-x[1]):
      print(k, v)
    sys.stdout = sys.__stdout__

  category_freq = collections.Counter([d.category if d.category else '-' for d in dev.values()])
  with open(target_dir + '/category_freq.dev.txt', 'w') as f:
    sys.stdout = f
    for k, v in sorted(category_freq.items(), key=lambda x:-x[1]):
      print(k, v)
    sys.stdout = sys.__stdout__

  category_freq = collections.Counter([d.category if d.category else '-' for d in test.values()])
  with open(target_dir + '/category_freq.test.txt', 'w') as f:
    sys.stdout = f
    for k, v in sorted(category_freq.items(), key=lambda x:-x[1]):
      print(k, v)
    sys.stdout = sys.__stdout__


  if not os.path.exists(target_dir):
    os.makedirs(target_dir)

  dump_as_json(train, os.path.join(target_dir, 'train.jsonlines'))
  dump_as_json(dev, os.path.join(target_dir, 'dev.jsonlines'))
  dump_as_json(test, os.path.join(target_dir, 'test.jsonlines'))

  dump_as_json(train, os.path.join(target_dir, 'train.json'), as_jsonlines=False)
  dump_as_json(dev, os.path.join(target_dir, 'dev.json'), as_jsonlines=False)
  dump_as_json(test, os.path.join(target_dir, 'test.json'), as_jsonlines=False)



if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('-ntr', '--n_train', type=int, default=0)
  parser.add_argument('-ndv', '--n_dev', type=int, default=3000)
  parser.add_argument('-nts', '--n_test', type=int, default=3000)

  parser.add_argument('-s', '--source_dir',
                      default='wikiP2D/wikiP2D.p1s0',
                      help='')
  parser.add_argument('-t', '--target_dir', default='')
  parser.add_argument('-f', '--filename', default='merged.jsonlines')
  parser.add_argument('-mr', '--max_rows', type=int, default=0)
  parser.add_argument('-cs', '--category_size', type=int, default=500)
  parser.add_argument('-ml', '--min_n_links', type=int, default=3)
  parser.add_argument('-nr', '--n_relations', type=int, default=400)
  
  parser.add_argument('-mc', '--n_max_context', type=int, default=10)
  parser.add_argument('-us', '--uniform_sample', type=common.str2bool, 
                      default=True)

  parser.add_argument('--debug', type=common.str2bool, default=False)
  args = parser.parse_args()
  main(args)
