# coding: utf-8
from orderedset import OrderedSet
from pprint import pprint
from collections import OrderedDict, defaultdict, Counter
import multiprocessing as mp
import argparse, sys, os, time, json, commands, re, itertools, random, math
from common import str2bool, timewatch, multi_process, flatten

# plot statistics
import numpy as np
import matplotlib.pyplot as plt
try:
  import seaborn as sns
except:
  pass

try:
   import cPickle as pickle
except:
   import pickle

def pages_stats(pages, min_qfreq):
  w_count = defaultdict(int)
  q_count = defaultdict(int)
  def add_count(sent, link_spans):
    for w in sent.split():
      w_count[w] += 1
    for qid, _, _ in link_spans:
      q_count[qid] += 1

  for i, (pid, page) in enumerate(pages.items()):
    for j, para in enumerate(page):
      for h, (sent, link_spans) in enumerate(para):
        add_count(sent, link_spans)

  q_freq = sorted([(k, v) for k, v in q_count.items() if v >= min_qfreq], key = lambda x: -x[1])
  w_freq = sorted([(k, v) for k, v in w_count.items()], key = lambda x: -x[1])

  #plot([q_freq, w_freq], ['Entities', 'Words'])

  sys.stdout = sys.stderr
  #pprint(q_freq)
  sys.stdout = sys.__stdout__

  sys.stdout.write("Entity freq threshold: %d\n" % (args.min_qfreq))
  sys.stdout.write("Number of links: %d\n" % sum((n for _, n in q_freq)))
  sys.stdout.write("Number of words: %d\n" % sum((n for _, n in w_freq)))
  sys.stdout.write("Entity vocab size: %d\n" % len(q_freq))
  sys.stdout.write("Word vocab size: %d\n" % len(w_freq))

  return q_freq, w_freq

def plot(freqs, titles):
  def plot_hist(ax, freq, title=None):
    f = [x[1] for x in freq]
    width = 100
    bins = int((max(f) - min(f))/width)
    bins = 5000
    ax.hist(f, bins=bins)
    if title:
      ax.set_title(title)

  # https://stackoverflow.com/questions/6963035/pyplot-axes-labels-for-subplots
  plt.rcParams["font.size"] = 10
  fig = plt.figure()
  #plt.subplots_adjust(top=1.1)
  plt.subplots_adjust(wspace=0.4, hspace=0.3)
  ax = fig.add_subplot(111) 
  ax.tick_params(labelcolor='w', top='off', bottom='off', left='off', right='off')
  ax.set_xlabel('freq')
  ax.set_ylabel('N')
  n_plots = len(freqs)
  for i, (freq, title) in enumerate(zip(freqs, titles)):
    sub_ax = fig.add_subplot(n_plots, 1, i+1)
    sub_ax.set_xscale('log')
    sub_ax.set_yscale('log')
    plot_hist(sub_ax, freq, title)
  fig.savefig('/home/shoetsu/workspace/plot.eps')



def preprocess(pages):
  replace_tokens = [
    ('-LRB-', '('),
    ('-LSB-', '['),
    ('-LCB-', '{'),
    ('-RRB-', ')'),
    ('-RSB-', ']'),
    ('-RCB-', '}'),
  ]
  def _preprocess(text):
    for t1, t2 in replace_tokens:
      text = text.replace(t1, t2)
    text = text.decode('unicode-escape').encode('utf-8')
    return text
  res = OrderedDict()
  for k, paragraphs in pages.items():
    try:
      res[k] = [[(_preprocess(text), links) for text, links in sentences] for sentences in paragraphs]
    except:
      pass
  return res

@timewatch
def sum_by_qid(pages, min_qfreq):
  res = defaultdict(list)
  link_phrases = defaultdict(set)
  for paragraphs in pages.values():
    for sentences in paragraphs:
      for sentence in sentences:
        text, links = sentence
        for link in links:
          qid, start, end = link
          res[qid].append((text, start, end))
          link_phrases[qid].add(' '.join(text.split()[start:end+1]))
  res = OrderedDict(sorted([(k,v) for k,v in res.items() if len(v) >= min_qfreq], key=lambda x: len(-x[1])))

  link_phrases = {k:link_phrases[k] for k in res}
  return res, link_phrases

@timewatch
def read_dumps(dump_files, n_process):
  n_process = min(n_process, len(dump_files))
  pages = {}
  chunk_size = math.ceil((1.0 * len(dump_files) / n_process))
  pathes = [[x[1] for x in p] for i, p in itertools.groupby(enumerate(dump_files), lambda x: x[0] // chunk_size)]
  def load(pathes_per_process):
    res = {}
    dumps = [pickle.load(open(p, 'rb')) for p in pathes_per_process]
    for dump in dumps:
      res.update(dump)
    return res
  pages = multi_process(load, pathes)
  return pages

def check_linked_phrases(link_phrases, vocab):
  phrases = set(flatten([p for p in link_phrases.values()]))
  def oov_rate(vocab):
    tmp = set(vocab)
    cnt = 0
    for phrase in phrases:
      oov_words = [w for w in phrase.split() if w not in tmp]
      if oov_words:
        cnt += 1
        #print phrase.split(), [(w, vocab_dict[w]) for w in oov_words]
    return 1.0 * cnt / len(phrases)

  #sys.stdout.write('Number of Entities (N>=%d): %d\n' % (args.min_qfreq, len(link_phrases)))
  sys.stdout.write('Linked phrase vocab size: %d\n' % (len(phrases)))
  for N in [30000, 50000, 100000, 150000]:
    sys.stdout.write('OOV linked phrase rate (n_vocab=%d): %f\n' % (N, oov_rate(vocab[:N])))
  

def to_stderr(func):
  def wrapper(*args, **kwargs):
    sys.stdout = sys.stderr
    result = func(*args, **kwargs)
    sys.stdout = sys.__stdout__
    return result
  return wrapper


def select_from_od(od, n):
  res = OrderedDict()
  for k in od.keys()[:n]:
    res[k] = od[k]
  return res

@timewatch
def process_wikipedia(args):
  if not os.path.exists(args.target_dir + '/pages.min_q%d.bin' % args.min_qfreq) or args.cleanup:
    dump_dir = args.wp_source_dir + '/dumps'
    dump_files = commands.getoutput('ls -d %s/* | grep pages\..*\.bin.[0-9]' % dump_dir).split()
    if args.n_file:
      dump_files = dump_files[:args.n_file]
    dumps = read_dumps(dump_files, args.n_process) # list of pages per a process.
    pages = {}
    for d in dumps:
      pages.update(d)
    pages = preprocess(pages)
    q_freq, w_freq = pages_stats(pages, args.min_qfreq)
    vocab = [w for w, f in w_freq]

    data, link_phrases = sum_by_qid(pages, args.min_qfreq)
    check_linked_phrases(link_phrases, vocab)
    pickle.dump(data, open(args.target_dir + '/pages.min_q%d.bin' % args.min_qfreq, 'wb'))
  else:
    sys.stderr.write('Loading %s \n' % (args.target_dir + '/pages.min_q%d.bin' % args.min_qfreq))
    data = pickle.load(open(args.target_dir + '/pages.min_q%d.bin' % args.min_qfreq, 'rb'))
  return data

@timewatch
def combine_wikidata(subjects, items, props, triples, n_objects=0, n_props=300):

  tmp = triples
  freq_props = sorted(Counter([x[1] for x in tmp]).items(), key=lambda x: -x[1])
  n_props = n_props if n_props else len(freq_props)
  freq_props = OrderedDict(freq_props[:n_props])

  if n_objects:
    freq_objects = sorted(Counter([x[2] for x in tmp]).items(), key=lambda x: -x[1])
    n_objects = n_objects if n_objects else len(freq_objects)
    freq_objects = OrderedDict(freq_objects[:n_objects])
  else:
    freq_objects = OrderedSet(subjects.keys())

  # Create complete graph.
  selected_triples = [(s,r,o) for s,r,o in tmp if r in freq_props and o in freq_objects]

  # Remove unused props and unused object items.
  relations = OrderedDict()
  objects = OrderedDict()
  r_freq = Counter([r for s,r,o in triples])
  o_freq = Counter([o for s,r,o in triples])

  for k in freq_props:
    if k in r_freq:
      relations[k] = props[k] 
      relations[k]['freq'] = r_freq[k]

  for k in freq_objects:
    if k in used_objects:
      objects[k] = items[k] 
      objects[k]['freq'] = o_freq[k]

  summed_triples = defaultdict(list)
  for s,r,o in selected_triples:
    summed_triples[s].append((r, o))

  sys.stdout.write("(Objects:%d, Relation:%d) Number of Subjects, Props, Objects, Triples, = %d, %d, %d, %d\n" % (n_objects, n_props, len(subjects), len(relations), len(objects), len(selected_triples)))
  return relations, objects, triples_sum

def main(args):
  pages = process_wikipedia(args)

  wd_files = [os.path.join(args.wd_source_dir, w) for w in ['items.tokenized.bin', 'properties.tokenized.bin', 'triples.bin']]
  items, props, triples = (pickle.load(open(f, 'rb')) for f in wd_files)

  sys.stdout.write("(all) Number of Items, Props, Triples = %d, %d, %d\n" % (len(items), len(props), len(triples)))

  # Use only the triples where the entity linked in wikipedia is the subject.
  triples = [s for s,_,_ in triples if s in pages]
  s_freq = Counter([s for s,_,_ in triples])
  subjects = OrderedDict()
  for k in pages:
    if k in items:
      subjects[k] = items[k]
      subjects[k]['freq'] = s_freq[k]

  suffix = '.minq%d.bin' % (args.min_qfreq)
  pickle.dump(subjects, open(args.target_dir + '/subjects' + suffix, 'wb'))

  for n_objects in [0, 15000, 30000, 50000, 100000]:
    res = combine_wikidata(subjects, items, props, triples, 
                           n_objects=n_objects, n_props=300)
    relations, objects, summed_triples = res
    suffix = '.minq%d.o%dr%d.bin' % (args.min_qfreq, n_objects, n_props)
    pickle.dump(relations, open(args.target_dir + '/relations' + suffix, 'wb'))
    pickle.dump(objects, open(args.target_dir + '/objects' + suffix, 'wb'))
    pickle.dump(summed_triples, open(args.target_dir + '/triples' + suffix, 'wb'))


if __name__ == "__main__":
  desc = "This script creates wikiP2D corpus from Wikipedia dump sqls (page.sql, wbc_entity_usage.sql) and a xml file (pages-articles.xml) parsed by WikiExtractor.py (https://github.com/attardi/wikiextractor.git) with '--filter_disambig_pages --json' options."
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--wp_source_dir', default='wikipedia/latest/extracted/')
  parser.add_argument('--wd_source_dir', default='wikidata/latest/extracted/i100000p300/')
  parser.add_argument('--target_dir', default='wikiP2D')
  parser.add_argument('--min_qfreq', default=10, type=int)

  # optional 
  parser.add_argument('--n_process', default=8, type=int)
  parser.add_argument('--n_file', default=None, type=int)
  parser.add_argument('--cleanup', default=False, type=str2bool)
  args = parser.parse_args()
  main(args)