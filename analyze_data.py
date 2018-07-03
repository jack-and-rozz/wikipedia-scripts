# coding:utf-8
from collections import defaultdict, Counter
from pprint import pprint
import common
import argparse, collections, re, os, time, sys, codecs, json, random
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

@common.timewatch
def read_json(source_path):
  data = json.load(open(source_path)) 
  return common.recDotDict(data)

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

def main(args):
  data = read_json(args.file_path)
  

if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('file_path')
  args = parser.parse_args()
  main(args)
