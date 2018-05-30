#coding: utf-8
from pprint import pprint
import argparse, re, os, time, sys
from collections import OrderedDict
import common
try:
  import pickle as pickle
except:
   import pickle

def select_from_od(od, n):
  res = OrderedDict()
  for k in list(od.keys())[:n]:
    res[k] = od[k]
  return res

def str2bool(v):
  if type(v) == bool:
    return v
  return v.lower() in ("yes", "true", "t", "1")

def timewatch(func):
  def wrapper(*args, **kwargs):
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    print((func.__name__, ": ", end - start))
    return result
  return wrapper


def select_triples(triples, items, props):
  res = [(sbj, rel, obj) for (sbj, rel, obj) in triples if sbj in items and obj in items and rel in props]
  return res

def remove_unused_entities(triples, items, props):
  used_items = set([s for (s, r, o) in triples] + [o for (s, r, o) in triples])
  used_props = set([r for (s, r, o) in triples])
  unused_items = set(items.keys()) - used_items
  unused_props = set(props.keys()) - used_props
  i = OrderedDict()
  p = OrderedDict()
  for k, v in list(items.items()):
    if k not in unused_items:
      i[k] = v
  for k, v in list(props.items()):
    if k not in unused_props:
      p[k] = v
  return i, p

@timewatch
def main(args):
  print('----------------------')
  print("i%dp%d" % (args.max_items, args.max_props))
  target_dir = os.path.join(args.source_dir, "i%dp%d" % (args.max_items, args.max_props))
  s_items_fh = "%s/%s" % (args.source_dir, 'items')
  s_props_fh = "%s/%s" % (args.source_dir, 'properties')
  s_triples_fh = "%s/%s" % (args.source_dir, 'triples')
  t_items_fh = "%s/%s" % (target_dir, 'items')
  t_props_fh = "%s/%s" % (target_dir, 'properties')
  t_triples_fh = "%s/%s" % (target_dir, 'triples')

  if os.path.exists(t_items_fh + '.bin') and os.path.exists(t_props_fh + '.bin') and os.path.exists(t_triples_fh + '.bin'):
    sys.stderr.write('The subgraph is already built. Loading dump files from %s\n' % target_dir)
    items = pickle.load(open(t_items_fh + '.bin', 'rb'))
    props = pickle.load(open(t_props_fh + '.bin', 'rb'))
    triples = pickle.load(open(t_triples_fh + '.bin', 'rb'))
    for k,v in list(items.items()):
      print(k)
      pprint(v)
  else:
    # load data
    @timewatch
    def load_data():
      items = pickle.load(open(items_fh + '.bin', 'rb'))
      props = pickle.load(open(props_fh + '.bin', 'rb'))
      triples = pickle.load(open(triples_fh + '.bin', 'rb'))
      return items, props, triples

    items, props, triples = load_data()
    #print 'Original: '
    #pprint(items['Q486396'])
    #print "(original) items, props, triples = ", len(items), len(props), len(triples)
    items = select_from_od(items, args.max_items)
    props = select_from_od(props, args.max_props)
    triples = select_triples(triples, items, props)
    items, props = remove_unused_entities(triples, items, props)

    #print 'Processed: '
    #pprint(items['Q486396'])
    #print "(extracted) items, props, triples = ", len(items), len(props), len(triples)
    ## output results
    if not os.path.exists(target_dir):
      os.makedirs(target_dir)
    @timewatch
    def dump():
      pickle.dump(items, open(t_items_fh + ".bin", 'wb'))
      pickle.dump(props, open(t_props_fh + ".bin", 'wb'))
      pickle.dump(triples, open(t_triples_fh + ".bin", 'wb'))

    @timewatch
    def dump_as_text():
      with open(items_fh + ".txt", 'w') as f:
        for k, v in list(items.items()):
          columns = [k, v['name'], str(v['freq']), v['desc'], ",".join(v['aka'])]
          line = '\t'.join(columns) + '\n'
          f.write(line)

      with open(props_fh + ".txt", 'w') as f:
        for k, v in list(props.items()):
          columns = [k, v['name'], str(v['freq']), v['desc'], ",".join(v['aka'])]
          line = '\t'.join(columns) + '\n'
          f.write(line)

      with open(triples_fh + ".txt", 'w') as f:
        for t in triples:
          line = '\t'.join(t) + '\n'
          f.write(line)
    dump()
    dump_as_text()

if __name__ == "__main__":
  desc = 'This script is for preprocessing latest-truthy.nt file in \'https://dumps.wikimedia.org/wikidatawiki/entities/\''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', default='wikidata/latest/extracted')
  parser.add_argument('--max_items', default=100000, type=int)
  parser.add_argument('--max_props', default=300, type=int)
  args = parser.parse_args()
  main(args)
