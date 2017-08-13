#coding: utf-8
from pprint import pprint
import argparse, re, os, time, sys, random
from collections import OrderedDict

try:
   import cPickle as pickle
except:
   import pickle


def stanford_tokenizer(sentences):
  tmp_dir = '/tmp'
  file_path = os.path.join(tmp_dir, 'tokenizing')
  with open(file_path, 'w') as f:
    for sent in sentences:
      f.write(sent + '\n')
  cmd='java edu.stanford.nlp.process.PTBTokenizer -preserveLines < %s 2> /dev/null' % file_path
  tokenized = commands.getoutput(cmd).split('\n')
  os.system("rm %s" % file_path)
  return tokenized

def main(args):
  if not os.path.exists(args.source_dir + '/corpus')
  pass

if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('source_dir')
  parser.add_argument('--n_valid', default=100000, type=int)
  parser.add_argument('--n_test', default=300, type=int)
  args = parser.parse_args()
  main(args)
