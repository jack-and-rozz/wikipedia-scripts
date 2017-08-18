# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict, Counter
import multiprocessing as mp
import argparse, sys, os, time, json, commands, re, itertools, random
from common import str2bool, timewatch, multi_process

# plot statistics
import numpy as np
import matplotlib.pyplot as plt

try:
   import cPickle as pickle
except:
   import pickle

def pages_stats(pages):
  def plot_hist(ax, freq, title=None):
    f = [x[1] for x in freq]
    width = 100
    bins = int((max(f) - min(f))/width)
    bins = 10000
    ax.hist(f, bins=bins)
    if title:
      ax.set_title(title)

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

  q_freq = sorted([(k, v) for k, v in q_count.items() if v > args.min_qfreq], key = lambda x: -x[1])
  w_freq = sorted([(k, v) for k, v in w_count.items() if v > args.min_wfreq], key = lambda x: -x[1])

  fig = plt.figure()
  plt.subplots_adjust(top=0.75)
  ax = fig.add_subplot(111) 
  ax.set_xlabel('freq')
  ax.set_ylabel('N')
  plt.yscale('log')
  plot_hist(fig.add_subplot(2,1,1), q_freq, 'Entities')
  plot_hist(fig.add_subplot(2,1,2), w_freq, 'Words')
  fig.savefig('/home/shoetsu/workspace/plot.ylog.eps')

  sys.stdout = sys.stderr
  pprint(q_freq)
  sys.stdout = sys.__stdout__

  sys.stdout.write("Threshold: (Entity, Word) = (%d, %d)\n" % (args.min_qfreq, args.min_wfreq))
  sys.stdout.write("Entity vocab size: %d\n" % len(q_freq))
  sys.stdout.write("Number of links: %d\n" % sum((n for _, n in q_freq)))
  sys.stdout.write("Word vocab size: %d\n" % len(w_freq))
  sys.stdout.write("Number of words: %d\n" % sum((n for _, n in w_freq)))
  #pprint(Counter(count.values()).most_common())

@timewatch
def main(args):
  dump_dir = args.source_dir + '/dumps.bak'
  dump_files = commands.getoutput('ls -d %s/* | grep pages\..*\.bin.[0-9]' % dump_dir).split()
  
  pages = {}
  n_process = 8
  for i, p in itertools.groupby(enumerate(dump_files), lambda x: x[0] // n_process):
    pathes = [x[1] for x in p]
    dumps = multi_process(lambda f: pickle.load(open(f, 'rb')), pathes)
    for d in dumps:
      pages.update(d)
  pages_stats(pages)

if __name__ == "__main__":
  desc = "This script creates wikiP2D corpus from Wikipedia dump sqls (page.sql, wbc_entity_usage.sql) and a xml file (pages-articles.xml) parsed by WikiExtractor.py (https://github.com/attardi/wikiextractor.git) with '--filter_disambig_pages --json' options."
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', default='/home/shoetsu/disk/dataset/wikipedia/en/latest/extracted/')
  parser.add_argument('--min_qfreq', default=100, type=int)
  parser.add_argument('--min_wfreq', default=20, type=int)
  args = parser.parse_args()
  main(args)
