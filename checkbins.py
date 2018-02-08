import argparse
from common import timewatch
from pprint import pprint
from wp.extract_all import count
exit(1)
try:
   import cPickle as pickle
except:
   import pickle

@timewatch
def main(args):
  data = pickle.load(open(args.input_file, 'rb'))
  pprint(data)


if __name__ == '__main__':
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('input_file')
  args = parser.parse_args()
  main(args)