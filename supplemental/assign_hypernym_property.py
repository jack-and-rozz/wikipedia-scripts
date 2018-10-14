# coding:utf-8
import sys, os, random, copy, socket, time, re, argparse, json
from collections import OrderedDict, defaultdict
from pprint import pprint
from common import flatten, recDotDict


def read_jsonlines(source_path, max_rows=0):
  data = {}
  for i, l in enumerate(open(source_path)):
    if max_rows and i >= max_rows:
      break
    d = json.loads(l)
    data.update({d['qid']:d})
  return recDotDict(data)

def read_subprop_tree(subprop_tree_path):
  prop_tree = defaultdict(list)
  for i, l in enumerate(open(subprop_tree_path)):
    s, r, o = l.strip().split()
    if s[0] == 'P' and o[0] == 'P':
      prop_tree[s].append(o)
  all_keys = set(prop_tree.keys())
  for k in all_keys:
    while True:
      parents = prop_tree[k]
      grand_parent = flatten([prop_tree[parent] for parent in parents if parent in prop_tree])
      if grand_parent:
        prop_tree[k] = grand_parent
      else:
        break
  return prop_tree

# def read_subprop_tree(subprop_tree_path):
#   prop_tree = defaultdict(list)
#   for i, l in enumerate(open(subprop_tree_path)):
#     s, r, o = l.strip().split()
#     if s[0] == 'P' and o[0] == 'P':
#       prop_tree[s].append(o)
#   all_keys = set(prop_tree.keys())

#   def trace_root(prop_tree, k):
#     if k in prop_tree:
#       prop_tree[k] = flatten([trace_root(prop_tree, kk) for kk in prop_tree[k]])
#       return prop_tree[k]
#     else:
#       return []

#   for k in all_keys:
#     prop_tree[k] = trace_root(prop_tree, k)

#   return prop_tree

def main(args):
  subprop_tree = read_subprop_tree(args.subprop_tree_path)
  all_props = read_jsonlines(args.all_properties_path) 
  frequent_props = read_jsonlines(args.frequent_properties_path) 

  all_prop_keys = list(all_props.keys())
  for k in all_prop_keys:
    if k in subprop_tree:
      print ('%s -> %s' % ('%s(%s)' % (all_props[k].name, k), ', '.join(['%s(%s)' % (all_props[kk].name, kk) for kk in subprop_tree[k] if kk in all_props])))
      del all_props[k]

  print('# of all props', len(set(all_prop_keys)))
  print('# of all props after removing hyponymic props', len(set(all_props.keys())))
  print('# of hyponymic props ', len(set(subprop_tree.keys())))
  print('# of frequent props', len(set(frequent_props.keys())))
  print('# of frequent props after removing hyponymic props', len(set(all_props.keys()).intersection(set(frequent_props.keys()))))
  
  hypernyms = sorted([frequent_props[k] for k in set(all_props.keys()).intersection(set(frequent_props.keys()))], key= lambda x:-x.freq)
  
  pprint([(i, p.name, p.qid, p.freq) for i, p in enumerate(hypernyms)])
if __name__ == "__main__":
  # Common arguments are defined in base.py
  desc = ""
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--all_properties_path', 
                      default='wikiP2D/wikiP2D.p1s0/properties.tokenized.jsonlines',
                      type=str, help ='')
  parser.add_argument('--frequent_properties_path', 
                      default='wikiP2D/wikiP2D.p1s0/relex/properties.jsonlines',
                      type=str, help ='')
  parser.add_argument('--subprop_tree_path', 
                      default='wd.dumps.all/triples.subprops', type=str, help ='')
  args = parser.parse_args()
  main(args)
