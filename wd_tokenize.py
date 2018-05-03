from collections import OrderedDict, defaultdict
from common import str2bool, timewatch, multi_process
import argparse, sys, os, time, json, re, itertools, random, itertools, math

from pprint import pprint

try:
  import commands
except:
  import subprocess as commands
  
try:
#   import cPickle as pickle
  import _pickle as pickle
except:
   import pickle

LRB = '-LRB-'
LSB = '-LSB-'
RRB = '-RRB-'
RSB = '-RSB-'

# import corenlp
# corenlp_dir = os.path.join(os.environ['HOME'], 
#                            "workspace/downloads/stanford-corenlp")
# properties_file = os.path.join(corenlp_dir, 'user.properties')

# ############################################
# ##            Tokenizer
# ############################################

# def stanford_tokenizer(text, s_parser):
#   if text == "" or text == " ":
#     return ""
#   result = json.loads(s_parser.parse(text))
#   sentences = [sent for sent in result['sentences'] if sent['words']]
#   para = [" ".join([w[0] for w in sent['words']]).encode('utf-8') for sent in sentences]
#   if para:
#     return para[0]
#   else:
#     return ""

# def parse_entity_texts(entities):
#   parser = corenlp.StanfordCoreNLP(corenlp_path=corenlp_dir, properties=properties_file)
#   res = entities
#   #res = {}
#   for k in entities:
#     name = entities[k]['name']
#     desc = entities[k]['desc']
#     aka = entities[k]['aka']
#     res[k] = {}
#     res[k]['name'] = stanford_tokenizer(name, parser)
#     res[k]['desc'] = stanford_tokenizer(desc, parser)
#     res[k]['aka'] = [stanford_tokenizer(x, parser) for x in aka]
#   return res

# @timewatch
# def multi_parse(entities):
#   chunk_size = math.ceil((1.0 * len(entities) / args.n_process))
#   all_keys = [[x[1] for x in p] for i, p in itertools.groupby(enumerate(entities), lambda x: x[0] // chunk_size)]
#   entities = [{k:entities[k] for k in keys} for keys in all_keys]

#   tmp = {}
#   for d in multi_process(parse_entity_texts, entities):
#     tmp.update(d)
#   return tmp
#   res = OrderedDict()

#   for k, _ in sorted([(k, v['freq']) for k, v in tmp.items()], key=lambda x: -x[1]):
#     res[k] = tmp[k]
#   return res


def preprocess(sent):
  # Remove the phrases in parenthesis.
  pattern = '\s?\(.+?\)'
  for m in re.findall(pattern, sent):
    sent = sent.replace(m, '')
  pattern = '\s?\[.+?\]'
  for m in re.findall(pattern, sent):
    sent = sent.replace(m, '')
  return sent

def dump_as_text(file_path, entities):
  with open(file_path, 'w') as f:
    for k, v in entities.items():
      name = v['name']
      desc = v['desc']
      freq = str(v['freq'])
      aka = v['aka']
      if type(aka) == list or type(aka) == set:
        aka = ",".join([x for x in aka])
      columns = [k, name, freq, desc, aka]
      line = '\t'.join(columns) + '\n'
      #f.write(line.encode('utf-8'))
      try:
        f.write(line.encode('utf-8'))
      except Exception as e:
        print e
        print type(name), name
        print type(freq), freq
        print type(desc), desc
        print type(aka), aka
        print type(line), line
        exit(1)

@timewatch
def output_and_parse(entities, file_path):
  # Output the original .bin file as a .txt file and parse it by stanford tokenizer.
  for k, v in entities.items():
    entities[k]['desc'] = preprocess(entities[k]['desc']).decode('unicode-escape')
    entities[k]['aka'] = preprocess(' , '.join(entities[k]['aka'])).decode('unicode-escape')
    entities[k]['name'] = entities[k]['name'].decode('unicode-escape')
  if not os.path.exists(file_path) or args.cleanup:
    dump_as_text(file_path, entities)

  #cmd = ' %s | java edu.stanford.nlp.process.PTBTokenizer -preserveLines ' % (file_path)
  cmd = '%s' % (file_path)
  name = commands.getoutput('cut -f2 ' + cmd).split('\n')
  desc = commands.getoutput('cut -f4 ' + cmd).split('\n')
  aka = commands.getoutput('cut -f5 ' + cmd).split('\n')
  log = "%d %d %d %d\n" %(len(entities), len(name), len(desc), len(aka))
  sys.stderr.write(log)
  #exit(1)
  i = 0
  for qid, n,d,a in zip(entities, name, desc, aka):
    entities[qid]['name'] = n
    entities[qid]['desc'] = d
    entities[qid]['aka'] = set(a.split(' , '))
  return entities


@timewatch
def main(args):
  if not os.path.exists(args.source_dir + '/properties.tokenized.bin') or args.cleanup:
    props = pickle.load(open(args.source_dir + '/properties.bin', 'rb'))
    props = output_and_parse(props, args.source_dir + '/properties.txt')
    pickle.dump(props, open(args.source_dir + '/properties.tokenized.bin', 'wb'))
    #dump_as_text(args.source_dir + '/props.tokenized.txt', props)
  else:
    sys.stderr.write('Tokenized dump found. Loading %s\n' % os.path.join(args.source_dir, '/props.tokenized.bin'))
    props = pickle.load(open(args.source_dir + '/properties.tokenized.bin', 'rb'))


  if not os.path.exists(args.source_dir + '/items.tokenized.bin') or args.cleanup:
    items = pickle.load(open(args.source_dir + '/items.bin', 'rb'))
    qid = 'Q486396'
    print 'items.bin', qid, items[qid]

    items = output_and_parse(items, args.source_dir + '/items.txt')
    pickle.dump(items, open(args.source_dir + '/items.tokenized.bin', 'wb'))
    qid = 'Q486396'
    print 'items.bin', qid, items[qid]
    #dump_as_text(args.source_dir + '/items.tokenized.txt', items)
  else:
    sys.stderr.write('Tokenized dump found. Loading %s\n' % os.path.join(args.source_dir, '/items.tokenized.bin'))
    items = pickle.load(open(args.source_dir + '/items.tokenized.bin', 'rb'))


  #pprint(dict(props))
  #pprint()


if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', default='wikidata/latest/extracted/all')
  parser.add_argument('--cleanup', default=False, type=str2bool)
  args = parser.parse_args()

  main(args)
