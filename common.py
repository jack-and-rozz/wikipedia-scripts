# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict
from collections import OrderedDict, defaultdict
from getpass import getpass

import multiprocessing as mp
import argparse, sys, os, time, json, re, itertools, random, itertools

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
##              Utils
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

  # def bfsearch(self):
  #   for k in self:
  #     if isinstance(self[k], type(self)):
  #       yield self[k].bfsearch()
  #     else:
  #       yield self[k]
  #     #print k , type(self[k])
  #   exit(1)

def str2bool(v):
  if type(v) == bool:
    return v
  return v.lower() in ("yes", "true", "t", "1")

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


def flatten(l):
  return list(itertools.chain.from_iterable(l))

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



from inspect import currentframe
def dbgprint(*args):
  names = {id(v):k for k,v in currentframe().f_back.f_locals.items()}
  print(', '.join(names.get(id(arg),'???')+' = '+repr(arg) for arg in args))
