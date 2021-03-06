#coding: utf-8
from pprint import pprint
import argparse, collections, re, os, time, sys, codecs, regex, json
from common import str2arr, str2bool, timewatch, multi_process, flatten, dump_as_json, recDotDict, dbgprint
from common import setup_parser, stanford_tokenizer, read_json, read_jsonlines
from tqdm import tqdm

try:
   import pickle as pickle
except:
   import pickle

# Important column patterns that have a particular value or string.
name_url = '<http://schema.org/name>'
description_url = '<http://schema.org/description>'
aka_url = '<http://www.w3.org/2004/02/skos/core#altLabel>'

# An entity's name, description, and aka are followed by a lang symbol. (e.g. "green plants"@en)
value_template = '\"(.+)\"\s*@%s'

LRB = '-LRB-' # (
RRB = '-RRB-' # )
LSB = '-LSB-' # [
RSB = '-RSB-' # ]
LCB = '-LCB-' # {
RCB = '-RCB-' # }

n_line = 3165697222 # latest-truthy (at 2017-08-02)

def create_entity_template(qid):
  return {
    'name': '',
    'desc': '',
    'aka': [],
    #'triples': [],
    'freq': {
      'triple': 0
    },
    'qid': qid
  }

node_pattern = re.compile('[PQ][0-9]+')
def is_an_edge(triple):
  ids = [node_pattern.match(x.split('/')[-1][:-1]) for x in triple]
  if not None in ids:
    return [m.group(0) for m in ids]
  else:
    return None

def is_a_value(triple):
  rel = triple[1]
  return rel in [name_url, description_url, aka_url]

@timewatch
def read_wikidatadump(args):
  sys.stderr.write('Reading data from \'%s\'. This process will take 8-9 hours or more...\n' % args.source_path)
  entities = {}
  triples = []
  max_rows = args.max_rows if args.max_rows else n_line
  pbar = tqdm(range(min(n_line, max_rows)))
  value_pattern = re.compile(value_template % args.lang)
  for i, l in enumerate(open(args.source_path)):
    pbar.update(1)
    l = l.split(' ')
    subj_url, rel_url, obj_url = l[0], l[1], " ".join(l[2:-1])
    # Extract relations between items.
    triple = is_an_edge((subj_url, rel_url, obj_url))
    if triple:
      for x in triple:
        if not x in entities:
          entities[x] = create_entity_template(x)
        #entities[x]['triples'].append(triple)
      triples.append(triple)

      subj, rel, obj = triple
      if subj[0] == 'Q':
        entities[subj]['freq']['triple'] += 1
      if obj[0] == 'Q':
        entities[obj]['freq']['triple'] += 1
      entities[rel]['freq']['triple'] += 1

    # Extract values (name, description, aka.)
    elif is_a_value((subj_url, rel_url, obj_url)):
      idx = [name_url, description_url, aka_url].index(rel_url)
      v_type = ['name', 'desc', 'aka'][idx]

      m = value_pattern.match(obj_url)
      if m: # values must be written in target lang.
        v = m.group(1)
        v = ' '.join([x for x in v.split() if x]).strip()
        v = codecs.decode(v, 'unicode-escape')
      else:
        continue
      subj = subj_url.split('/')[-1][:-1] # "<.+/Q[0-9+]>" -> "Q[0-9+]"
      if not subj in entities:
        entities[subj] = create_entity_template(subj)
      if v_type == 'aka':
        entities[subj][v_type].append(v)
      else:
        entities[subj][v_type] = v

    if args.max_rows and i > args.max_rows:
      break
  pbar.close()
  return entities, triples

def remove_incomplete_entities_and_sort_by_freq(entities, required_value_types=[]):
  def is_complete(entity):
    n_filled_values = len([entity[v_type] for v_type in required_value_types if entity[v_type]])
    if len(required_value_types) == n_filled_values:
      return True
    else:
      return False

  if required_value_types:
    # Remove incomplete (no name) entities due to lack of data
    e_complete = [k for k in entities if is_complete(entities[k])]
    e_complete = {k:entities[k] for k in e_complete}
  else:
    e_complete = entities

  freq_items = sorted([(k, e_complete[k]['freq']['triple']) for k in e_complete if k[0] == 'Q'], key=lambda x: -x[1])
  freq_props = sorted([(k, e_complete[k]['freq']['triple']) for k in e_complete if k[0] == 'P'], key=lambda x: -x[1])
  items = collections.OrderedDict()
  props = collections.OrderedDict()
  for (k, _) in freq_items:
    items[k] = e_complete[k]
  for (k, _) in freq_props:
    props[k] = e_complete[k]
  return items, props

# NOTE: recursive regex expression sometimes never finish tracing (e.g. a phrase containing an expression in a one-sided parenthesis)
#rec_parentheses = regex.compile("(?<rec>\((?:[^\(\)]+|(?&rec))*\))")
#rec_brackets = regex.compile("(?<rec>\[(?:[^\[\]]+|(?&rec))*\])")
#rec_braces = regex.compile("(?<rec>\{(?:[^\{\}]+|(?&rec))*\})")
# NOTE2: regex.compile('\(([^()]|(?R))*\)') may be better?

parentheses = re.compile('\(.+\)')
brackets =  re.compile('\[.+\]')
braces =  re.compile('\{.+\}')
colons = re.compile('[:;].+')
def preprocess(sent):
  sent = parentheses.sub('', sent)
  sent = brackets.sub('', sent)
  sent = braces.sub('', sent)
  sent = colons.sub('', sent)
  return sent

def dump_triples(triples, triples_path):
  with open(triples_path, 'w') as f:
    for s,r,o in triples:
      line = '%s\t%s\t%s\n'  % (s,r,o)
      f.write(line)
  

def to_dict(jsonlist):
  res = collections.OrderedDict()
  for d in jsonlist:
    res[d['qid']] = d
  return res

@timewatch
def process_entities(args):

  items_fn = "%s/%s" % (args.output_dir, 'items') 
  props_fn = "%s/%s" % (args.output_dir, 'properties')
  entities_fn = "%s/%s" % (args.output_dir, 'entities') 
  triples_fn = "%s/%s" % (args.output_dir, 'triples') 

  if os.path.exists(items_fn + '.jsonlines.raw') and os.path.exists(props_fn + '.jsonlines.raw'):
    sys.stderr.write('Found an intermediate file \'%s\'.\n' % (items_fn + '.json.raw'))
    #items = read_jsonlines(items_fn + '.jsonlines.raw', _type=None)
    sys.stderr.write('Found an intermediate file \'%s\'.\n' % (props_fn + '.json.raw'))
    props = to_dict(read_jsonlines(props_fn + '.jsonlines.raw', _type=None))
    #return items, props
    return props

  entities, triples = read_wikidatadump(args)
  dump_triples(triples, triples_fn + '.txt')
  del triples

  #dump_as_json(entities, entities_fn + '.jsonlines.raw', True) # For backup.
  #dump_as_json(entities, entities_fn + '.json.raw', False) # For backup.

  items, props = remove_incomplete_entities_and_sort_by_freq(
    entities, args.required_value_types)

  #items, props = remove_triples_about_incomplete_entity(items, props)
  dump_as_json(items, items_fn + '.jsonlines.raw', True)
  dump_as_json(props, props_fn + '.jsonlines.raw', True)
  #dump_as_json(items, items_fn + '.json.raw', False)
  #dump_as_json(props, props_fn + '.json.raw', False)
  #return items, props
  del items
  return props

@timewatch
def tokenize(data, host, port):
  s_parser = setup_parser(host=host, port=port)
  pbar = tqdm(range(len(data)))
  for k in data:
    try:
      data[k]['desc'] = stanford_tokenizer(preprocess(data[k]['desc']), s_parser)[0]
    except:
      data[k]['desc'] = ''
    pbar.update(1)
  pbar.close()
  s_parser.close()
  return data

def tokenize_items(items, host, port, n_process):
  '''
  Do tokenization in multi-process for items.
  since to keep all the items as a variable requires a large amount of memories.
  '''
  n_per_process = len(items.keys()) // n_process + 1
  items = [(k, v) for k,v in items.items()]
  divided_items = [dict(items[i:i+n_per_process]) for i in range(0, len(items), n_per_process)]

  items = {}
  #dbgprint(len(divided_items))
  for r in multi_process(tokenize, divided_items, 
                         [host for _ in range(n_process)], 
                         [port for _ in range(n_process)]):
    items.update(r)
  return items


def tokenize_and_write(items, f, args):
  items = tokenize_items(items, args.corenlp_host, args.corenlp_port, 
                         args.n_process)
  for d in items.values():
    json.dump(d, f, ensure_ascii=False)
    f.write('\n')

@timewatch
def main(args):
  if not os.path.exists(args.output_dir):
    os.makedirs(args.output_dir)
  items_fn = "%s/%s" % (args.output_dir, 'items') 
  props_fn = "%s/%s" % (args.output_dir, 'properties')
  if (os.path.exists(items_fn + '.jsonlines') or os.path.exists(props_fn + '.jsonlines')) and not args.cleanup:
    sys.stderr.write('Output json files already exist. (%s, %s)\n' % (items_fn + '.jsonlines', props_fn + '.jsonlines'))
    return
  props = process_entities(args)

  # Parse property descriptions. 
  if not os.path.exists(props_fn + '.jsonlines'):
    props = tokenize(props, args.corenlp_host, args.corenlp_port)
    dump_as_json(props, props_fn + '.jsonlines', True)

  # Parse item descriptions. Since keeping all the entities consumes a large amount of memory, thus this script reloads item.jsonlines.raw by chunk and process it.
  if not os.path.exists(items_fn + '.jsonlines'):
    with open(items_fn + '.jsonlines.raw') as fs:
      with open(items_fn + '.jsonlines', 'a') as ft:
        items = collections.OrderedDict()
        for i, l in enumerate(fs):
          d = json.loads(l)
          items[d['qid']] = d
          if len(items) >= args.chunk_size:
            tokenize_and_write(items, ft, args)
            items = collections.OrderedDict()
        tokenize_and_write(items, ft, args)

      #dump_as_json(items, items_fn + '.jsonlines', True)
      #dump_as_json(items, items_fn + '.json', False)
  sys.stderr.write('Processed files : items.jsonlines, properties.jsonlines, triples.txt\n')

if __name__ == "__main__":
  desc = 'This script is for preprocessing latest-truthy.nt file downloaded from \'https://dumps.wikimedia.org/wikidatawiki/entities/\'.'
  parser = argparse.ArgumentParser(description=desc)

  # Input and Output dirs
  parser.add_argument('-s', '--source_path', 
                      default='wikidata/latest/latest-truthy.nt')
  parser.add_argument('-o', '--output_dir', 
                      default='wikidata/latest/extracted')
  parser.add_argument('-l', '--lang', default='en')
  parser.add_argument('-mr', '--max_rows', default=None, type=int, 
                      help='The maximum lines to be read from the original dump. Mainly for debugging.')
  parser.add_argument('--required_value_types', default='name', type=str2arr,
                      help='a list-string of required value types which an entity must have (delimited by ",").')

  # MultiProcessing
  parser.add_argument('-npr','--n_process', default=10, type=int)
  parser.add_argument('-cs', '--chunk_size', default=1000000, type=int)

  # CoreNLP
  parser.add_argument('-ch', '--corenlp_host', default='http://localhost',
                      type=str)
  parser.add_argument('-cp', '--corenlp_port', default=9000, type=int)

  parser.add_argument('--cleanup', default=False, type=str2bool)
  args = parser.parse_args()
  main(args)
