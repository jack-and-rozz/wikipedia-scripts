#coding:utf-8
from collections import defaultdict, Counter
from pprint import pprint
import common
from common import RED, RESET, BLUE, BOLD, UNDERLINE
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

def count(data, props):
  cnt_triple_pair_freq = defaultdict(int)
  cnt_triple_rel_freq = defaultdict(int)
  cnt_triple_obj_freq = defaultdict(int)

  cnt_title_subj_triple = 0
  cnt_title_obj_triple = 0
  for a_qid, d in data.items():
    text = d.text if type(d.text[0]) == str else d.text[0]

    for s, r, o in d.triples:
      rel = props[r].name
      if s != a_qid: # (title, rel, ?) 
        cnt_title_obj_triple += 1
        _, s_id, (begin, end) = d.link[s]
        obj = d.title
        subj = ' '.join([w for w in text[s_id].split()[begin:end+1]]).lower()
      else:
        cnt_title_subj_triple += 1
        _, s_id, (begin, end) = d.link[o]
        subj = d.title
        obj = ' '.join([w for w in text[s_id].split()[begin:end+1]]).lower()
        cnt_triple_pair_freq[(rel, obj)] += 1
        cnt_triple_rel_freq[rel] += 1
        cnt_triple_obj_freq[obj] += 1
  print ('# of Title subj triples:\t%d' % cnt_title_subj_triple)
  print ('# of Title obj triples:\t%d' % cnt_title_obj_triple)

  def _sort(cnt):
    if isinstance(cnt, dict):
      cnt = [(k,v) for k,v in cnt.items()]
    cnt = sorted(cnt, key=lambda x: -x[1])
    return cnt
  print ('Frequent rel-obj pairs:')
  print (_sort(cnt_triple_pair_freq)[:50])
  print ('')

  print ('Frequent rel')
  print (_sort(cnt_triple_rel_freq)[:50])
  print ('')
  print ('Frequent obj')
  print (_sort(cnt_triple_obj_freq)[:50])
  print ('')

def main(args):
  data = read_jsonlines(args.source_path, args.max_rows)
  props = read_jsonlines(args.props_path)
  count(data, props)
  
if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)

  parser.add_argument('-s', '--source_path',
                      default='wikiP2D/wikiP2D.p1s0/merged.jsonlines')
  parser.add_argument('-p', '--props_path',
                      default='wikiP2D/wikiP2D.p1s0/properties.tokenized.jsonlines')
  parser.add_argument('-mr', '--max_rows', type=int, default=0)
  args = parser.parse_args()
  main(args)
