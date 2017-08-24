#coding: utf-8
from pprint import pprint
import argparse, collections, re, os, time, sys

try:
   import cPickle as pickle
except:
   import pickle

# for important columns that have a particular value or string.
name_url = '<http://schema.org/name>'
description_url = '<http://schema.org/description>'
aka_url = '<http://www.w3.org/2004/02/skos/core#altLabel>'
# proptype_url = '<http://wikiba.se/ontology-beta#propertyType>' # unused

# the values of name, description, and aka are followed by a lang symbol. (e.g. "green plants"@en)
value_template = '\"(.+)\"\s*@%s' 



def str2bool(v):
  if type(v) == bool:
    return v
  return v.lower() in ("yes", "true", "t", "1")

def str2arr(v):
  return v.split(',')

def create_entity_template():
  return {
    'name': '',
    'desc': '',
    'aka': [],
    'freq': 0,
  }

def is_a_relation(triple):
  pattern = '[PQ][0-9]+'
  ids = [re.match(pattern, x.split('/')[-1][:-1])
         for x in triple]
  if not None in ids:
    return [m.group(0) for m in ids]
  else:
    return None

def is_a_value(triple):
  rel = triple[1]
  return rel in [name_url, description_url, aka_url]

def read_data(args):
  triples = []
  entities = {}

  for i, l in enumerate(open(args.source_path)):
    l = l.split(' ')
    subj_url, rel_url, obj_url = l[0], l[1], " ".join(l[2:-1])
    # Extract only (entity, prop, entity) or (entity | prop, type, value) triples.
    triple = is_a_relation((subj_url, rel_url, obj_url))
    if triple:
      triples.append(triple) 
      for x in triple:
        if not x in entities:
          entities[x] = create_entity_template()
      subj, rel, obj = triple
      if subj[0] == 'Q' or args.count_subj_prop:
        entities[subj]['freq'] += 1
      if obj[0] == 'Q' or args.count_obj_prop:
        entities[obj]['freq'] += 1
      entities[rel]['freq'] += 1

    # Extract important values (name, description, aka.)
    elif is_a_value((subj_url, rel_url, obj_url)):
      idx = [name_url, description_url, aka_url].index(rel_url)
      v_type = ['name', 'desc', 'aka'][idx]
      value = value_template % args.lang
      m = re.match(value, obj_url)
      if m: # values must be written in target lang.
        v = m.group(1)
      else:
        continue
      subj = subj_url.split('/')[-1][:-1] # "<.+/Q[0-9+]>" -> "Q[0-9+]"
      if not subj in entities:
        entities[subj] = create_entity_template()
      if v_type == 'aka':
        entities[subj][v_type].append(v)
      else:
        entities[subj][v_type] = v
    if i % 10000000 == 0:
      n_line = 1470298276 # latest-truthy (at 2017-08-02)
      percent = (i+1) * 100.0 / n_line
      sys.stderr.write("\rProgress rate: %f%% (%d/%d) " % (percent, i+1, n_line))
      sys.stderr.flush()

    if args.max_rows and i > args.max_rows:
      break

  sys.stderr.write("\rProgress rate: %f%% (%d/%d) \n" % (100, i+1, n_line))
  sys.stderr.flush()
  return entities, triples



def timewatch(func):
  def wrapper(*args, **kwargs):
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    print (func.__name__, ": ", end - start)
    return result
  return wrapper



def sort_by_freq(entities, required_value_types=[]):
  def is_complete(entity):
    if False not in [True if entities[k][v_type] else False for v_type in required_value_types]:
      return True
    else:
      return False
    
  if required_value_types:
    # Remove incomplete (no name) entities due to lack of data
    e_complete = [k for k in entities if is_complete(k)]
    e_complete = {k:entities[k] for k in e_complete}
  else:
    e_complete = entities

  freq_items = sorted([(k, e_complete[k]['freq']) for k in e_complete if k[0] == 'Q'], key=lambda x: -x[1])
  freq_props = sorted([(k, e_complete[k]['freq']) for k in e_complete if k[0] == 'P'], key=lambda x: -x[1])
  items = collections.OrderedDict()
  props = collections.OrderedDict()
  for (k, _) in freq_items:
    items[k] = e_complete[k]
  for (k, _) in freq_props:
    props[k] = e_complete[k]
  return items, props

@timewatch
def main(args):
  ent_fn = "%s/%s" % (args.target_dir, 'entities') + '.bin'
  items_fn = "%s/%s" % (args.target_dir, 'items') + '.bin'
  props_fn = "%s/%s" % (args.target_dir, 'properties') + '.bin'
  tri_fn = "%s/%s" % (args.target_dir, 'triples') + '.bin'
  # 保存する時はitemとpropで別にすべき？
  if os.path.exists(items_fn) and os.path.exists(props_fn) and os.path.exists(tri_fn) and not args.cleanup:
    #entities = pickle.load(open(ent_fn, 'rb'))
    #items = pickle.load(open(items_fn, 'rb'))
    #props = pickle.load(open(props_fn, 'rb'))
    #triples = pickle.load(open(tri_fn, 'rb'))
    pass
  else:
    entities, triples = read_data(args)
    items, props = sort_by_freq(entities, args.remove_anonymous_entities)
    #entities, triples = validate_kb(entities, triples)
    if not os.path.exists(args.target_dir):
      os.makedirs(args.target_dir)
    if not os.path.exists(items_fn):
      pickle.dump(items, open(items_fn, 'wb'))
    if not os.path.exists(props_fn):
      pickle.dump(props, open(props_fn, 'wb'))
    if not os.path.exists(tri_fn):
      pickle.dump(triples, open(tri_fn, 'wb'))
    if not os.path.exists(ent_fn):
      pickle.dump(entities, open(ent_fn, 'wb'))

    print 'items, props, triples = (%d, %d, %d)' % (len(items), len(props), len(triples))

if __name__ == "__main__":
  desc = 'This script is for preprocessing latest-truthy.nt file in \'https://dumps.wikimedia.org/wikidatawiki/entities/\''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--target_dir', default='wikidata/latest/extracted')
  parser.add_argument('--source_path', default='wikidata/latest/latest-truthy.nt')  
  parser.add_argument('--lang', default='en')
  parser.add_argument('--max_rows', default=None, type=int)
  parser.add_argument('--count_subj_prop', default=False, type=str2bool)
  parser.add_argument('--count_obj_prop', default=False, type=str2bool)
  parser.add_argument('--cleanup', default=False, type=str2bool)
  parser.add_argument('--required_value_types', default='name', type=str2arr,
                help='a list-string of required (a node must have) value types (delimited by ",").')
  args = parser.parse_args()
  main(args)
