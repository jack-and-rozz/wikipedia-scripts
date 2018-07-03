# coding:utf-8
from collections import defaultdict, Counter
from pprint import pprint
import common
import argparse, collections, re, os, time, sys, codecs, json, random

def read_jsonlines(source_path, max_rows=0):
  data = {}
  for i, l in enumerate(open(source_path)):
    if max_rows and i >= max_rows:
      break
    d = json.loads(l)
    data.update({d['qid']:d})
  return common.recDotDict(data)

def color_link(data):
  RED = "\033[31m"
  BLACK = "\033[30m"
  UNDERLINE = '\033[4m'
  BOLD = "\033[1m" + UNDERLINE
  RESET = "\033[0m"
  def _color_link(text, links):
    '''
    text : A list of sentence. a sentence is a list of words, or a string.
    '''
    if isinstance(text[0], str):
      text = [s.split() for s in text]
      #print(text)
    for sent_id, start, end in links:
      text[sent_id][start] = RED + text[sent_id][start]
      text[sent_id][end] = text[sent_id][end] + RESET
    return text
  new_data = {}
  link_fail_cnt = 0
  for qid, d in data.items():
    text = d.text[0]
    links = [(sent_id, start, end) for para_id, sent_id, (start, end) in d.link.values()]
    try:
      d.text = _color_link(text, links)
    except:
      print ('-----------------------')
      link_fail_cnt += 1 
      print (d.qid)
      print([s.split() for s in d.text[0]])
      pprint(d.link)
      print(len(text), [len(s.split()) for s in text])
    new_data[qid] = d
  print (len(new_data), link_fail_cnt)
  exit(1)
  
  return common.recDotDict(new_data)

def main(args):
  data = read_jsonlines(args.source_path, args.max_rows)
  data = color_link(data)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('source_path')
  parser.add_argument('-mr', '--max_rows', default=10000, type=int)
  args = parser.parse_args()
  main(args)
