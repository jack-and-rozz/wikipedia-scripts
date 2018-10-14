
from collections import defaultdict, Counter
from pprint import pprint
from common import read_jsonlines
import argparse, collections, re, os, time, sys, codecs, json, random, copy
random.seed(0)
from common import RED, RESET, BLUE, BOLD, UNDERLINE, GREEN


def main(args):
  data = read_jsonlines(args.source_path, args.max_rows)
  for qid, d in data.items():
    print(GREEN+ qid, RED+d.title + RESET)
    for text, (begin, end) in d.contexts:
      context = [BLUE+w+RESET if i in range(begin, end+1) else w for i, w in enumerate(text.split())]
      context = ' '.join(context)
      print(' -' + context)
    print()

  pass

if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('-s', '--source_path',
                      default='wikiP2D/wikiP2D.p1s0/desc_and_category/train.jsonlines', help='')
  parser.add_argument('-mr', '--max_rows', type=int, default=0)
  args = parser.parse_args()
  main(args)
