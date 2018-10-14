# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict
from getpass import getpass

import multiprocessing as mp
import argparse, sys, os, time, json, re, itertools, random, itertools
from stanfordcorenlp import StanfordCoreNLP
from inspect import currentframe

############################################
##         Color Constants
############################################


RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
BLACK = "\033[30m"
UNDERLINE = '\033[4m'
BOLD = "\033[1m" + UNDERLINE
RESET = "\033[0m"


############################################
##              Dict
############################################

class dotDict(dict):
  __getattr__ = dict.__getitem__
  __setattr__ = dict.__setitem__
  __delattr__ = dict.__delitem__

class recDotDict(dict):
  __getattr__ = dict.__getitem__
  __setattr__ = dict.__setitem__
  __delattr__ = dict.__delitem__
  def __init__(self, _dict={}):
    for k in _dict:
      if isinstance(_dict[k], dict):
        _dict[k] = recDotDict(_dict[k])
      if isinstance(_dict[k], list):
        for i,x in enumerate(_dict[k]):
          if isinstance(x, dict):
            _dict[k][i] = dotDict(x)
    super(recDotDict, self).__init__(_dict)

class rec_dotdefaultdict(defaultdict):
  __getattr__ = dict.__getitem__
  __setattr__ = dict.__setitem__
  __delattr__ = dict.__delitem__
  def __init__(self, _=None):
    super(rec_dotdefaultdict, self).__init__(rec_dotdefaultdict)


############################################
##              List
############################################

def flatten(l):
  return list(itertools.chain.from_iterable(l))


############################################
##              Json
############################################

def read_jsonlines(source_path, max_rows=0, _type=recDotDict):
  data = []
  for i, l in enumerate(open(source_path)):
    if max_rows and i >= max_rows:
      break
    d = _type(json.loads(l)) if _type else json.loads(l)
    data.append(d)
  return data

def read_json(source_path, _type=recDotDict):
  data = json.load(open(source_path)) 
  return _type(data) if _type else data

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

def json2jsonlines(source_path, data=None):
  assert source_path[-4:] == 'json'
  if not data:
    data = json.load(open(source_path))
  dump_as_json(data, source_path + 'lines', as_jsonlines=True)
  
############################################
##              String
############################################

def separate_path_and_filename(file_path):
    pattern = '^(.+)/(.+)$'
    m = re.match(pattern, file_path)
    if m:
      path, filename = m.group(1), m.group(2) 
    else:
      path, filename = None , file_path
    return path, filename

def str2arr(v):
  return [x for x in v.split(',') if x]

def str2bool(v):
  if type(v) == bool:
    return v
  return v.lower() in ("yes", "true", "t", "1")


############################################
##              Others
############################################
def dbgprint(*args):
  names = {id(v):k for k,v in currentframe().f_back.f_locals.items()}
  print(', '.join(names.get(id(arg),'???')+' = '+repr(arg) for arg in args))


def timewatch(func):
  def wrapper(*args, **kwargs):
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    sys.stderr.write(func.__name__ +  ": %f sec. \n" % (end - start))
    return result
  return wrapper

def multi_process(func, *args):
  '''
  Args:
    - func : a function to be executed.
    - args : a list of list of args that a worker needs. 
             [[id1, name1, ...], [id2, name2, ...]]
  '''
  # A wrapper to make a function put its response to a queue.
  def wrapper(_func, idx, q): 
    def _wrapper(*args, **kwargs):
      res = func(*args, **kwargs)
      return q.put((idx, res))
    return _wrapper
  workers = []
  # mp.Queue() seems to have a bug..? 
  # (stackoverflow.com/questions/13649625/multiprocessing-in-python-blocked)
  q = mp.Manager().Queue() 
  
  # kwargs are not supported... (todo)
  for i, a in enumerate(zip(*args)):
    worker = mp.Process(target=wrapper(func, i, q), args=a)
    workers.append(worker)
    worker.daemon = True  # make interrupting the process with ctrl+c easier
    worker.start()

  for worker in workers:
    worker.join()
  results = []
  while not q.empty():
    res = q.get()
    results.append(res)

  return [res for i, res in sorted(results, key=lambda x: x[0])]




############################################
##            Stanford CoreNLP
############################################

def setup_parser(host='http://localhost', port='9000'):
  return StanfordCoreNLP(host, port)

def stanford_tokenizer(text, s_parser, props={'annotators': 'tokenize,ssplit'}):
  text = text.replace('%', '% ') # to espace it from percent-encoding.
  result = json.loads(s_parser.annotate(text, props))
  sentences = [' '.join([tokens['word'] for tokens in sent['tokens'] if tokens['word']]) for sent in result['sentences']]
  return sentences
