import argparse, sys
from common import timewatch, str2bool
from pprint import pprint
from wp_extract_all import count, BOLD, RED, RESET, color_link
from wp_combine_wd import read_dumps
try:
   import cPickle as pickle
except:
   import pickle
import commands

def print_article(qid, article):
  for para_idx, paragraph in enumerate(article):
    for sent_idx, (sent, link_spans) in enumerate(paragraph):
      idx = "(%s-%d-%d)" % (qid, para_idx, sent_idx)
      sys.stdout.write("%s%s Processed%s: %s\n" % (
        BOLD, idx, RESET, color_link(sent, link_spans)))
      sys.stdout.write("%s%s Links%s: %s\n" % (
        BOLD, idx, RESET, link_spans))
      sys.stdout.write("\n")

@timewatch
def read(pathes):
  data = {}
  for p in pathes:
    data.update(pickle.load(open(p, 'rb')))
  return data

@timewatch
def read2(pathes):
  return read_dumps(pathes, args.n_processes)

@timewatch
def main(args):
  cmd = 'ls -d %s/pages.bin.*' % args.source_dir
  pathes = commands.getoutput(cmd).split('\n')[::-1]

  if args.max_files:
    pathes = pathes[:args.max_files]

  data = read(pathes)
  print type(data), len(data)

  if args.print_all:
    for qid, article in data.items():
      print_article(qid, article)

  count(data)
  #pprint(data)


if __name__ == '__main__':
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('source_dir')
  parser.add_argument('-m', '--max_files', default=0, type=int)
  parser.add_argument('-n', '--n_processes', default=1, type=int)
  parser.add_argument('-p', '--print_all', default=True, type=str2bool)


  args = parser.parse_args()
  main(args)