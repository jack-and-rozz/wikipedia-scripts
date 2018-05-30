# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict
from collections import OrderedDict, defaultdict
from getpass import getpass

import multiprocessing as mp
import argparse, sys, os, time, json, re, itertools, random, itertools


############################################
##              Utils
############################################

class dotdict(dict):
  __getattr__ = dict.__getitem__
  __setattr__ = dict.__setitem__
  __delattr__ = dict.__delitem__


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

