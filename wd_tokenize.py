from collections import OrderedDict, defaultdict
from common import str2bool, timewatch, multi_process
import argparse, sys, os, time, json, re, itertools, random, itertools, math

from pprint import pprint

try:
  import subprocess
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



def dump_as_text(file_path, entities):
  with open(file_path, 'w') as f:
    for k, v in list(entities.items()):
      name = v['name']
      desc = v['desc']
      freq = str(v['freq'])
      aka = v['aka']
      if type(aka) == list or type(aka) == set:
        aka = ",".join([x for x in aka])
      columns = [k, name, freq, desc, aka]
      line = '\t'.join(columns) + '\n'
      try:
        f.write(line)
      except Exception as e:
        print(e)
        print(type(name), name)
        print(type(freq), freq)
        print(type(desc), desc)
        print(type(aka), aka)
        print(type(line), line)
        exit(1)

def preprocess(sent):
  # Remove the phrases in parenthesis.
  pattern = '\s?\(.+?\)'
  for m in re.findall(pattern, sent):
    sent = sent.replace(m, ' ')
  pattern = '\s?\[.+?\]'
  for m in re.findall(pattern, sent):
    sent = sent.replace(m, ' ')
  sent = sent.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
  sent = ' '.join([x for x in sent.split()]).strip()
  return sent

def postprocess(sent):
  sent = sent.replace('\t', ' ').replace(LRB, '(').replace(RRB, ')').replace(LSB, '{').replace(RSB, '}').strip()
  return sent


@timewatch
def output_and_parse(source_filepath, text_filepath):
  # Output the original .bin file as a .txt file and parse it by stanford tokenizer.

  if not os.path.exists(text_filepath) or args.cleanup:
    entities = pickle.load(open(source_filepath, 'rb'))
    for k, v in list(entities.items()):
      entities[k]['desc'] = preprocess(entities[k]['desc'])
      entities[k]['aka'] = preprocess(' , '.join(entities[k]['aka']))
      name = entities[k]['name']
      name = name.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
      name = ' '.join([x for x in name.split()]).strip()
      entities[k]['name'] = name
    dump_as_text(text_filepath, entities)
  else:
    entities = OrderedDict()

  # Cut the stored file.
  qids = subprocess.getoutput('cut -f1 %s' % text_filepath).split('\n')
  freq = subprocess.getoutput('cut -f3 %s' % text_filepath).split('\n')
  freq = [int(x) for x in freq]

  # Cut the stored file and apply Tokenizer.
  tmp_filepath = text_filepath + '.name'
  cmd_tmp = 'cut -f%d %s | java edu.stanford.nlp.process.PTBTokenizer -preserveLines > %s 2>/dev/null'
  if not os.path.exists(tmp_filepath):
    cmd = cmd_tmp % (2, text_filepath, tmp_filepath)
    subprocess.getoutput(cmd)
  name = subprocess.getoutput('cat %s' % tmp_filepath).split('\n')
  name = [postprocess(x) for x in name]

  tmp_filepath = text_filepath + '.desc'
  if not os.path.exists(tmp_filepath):
    cmd = cmd_tmp % (4, text_filepath, tmp_filepath)
    subprocess.getoutput(cmd)
  desc = subprocess.getoutput('cat %s' % tmp_filepath).split('\n')
  desc = [postprocess(x) for x in desc]

  tmp_filepath = text_filepath + '.aka'
  if not os.path.exists(tmp_filepath):
    cmd = cmd_tmp % (5, text_filepath, tmp_filepath)
    subprocess.getoutput(cmd)
  aka = subprocess.getoutput('cat %s' % tmp_filepath).split('\n')
  aka = [postprocess(x) for x in aka]

  log = "qids, name, desc, aka = %d %d %d %d\n" % (len(qids), len(name), len(desc), len(aka))
  sys.stderr.write(log)

  assert len(qids) == len(name) == len(desc) == len(aka)
  i = 0

  for qid, n, f, d, a in zip(qids, name, freq, desc, aka):
    if not qid in entities:
      entities[qid] = {}
    entities[qid]['qid'] = qid
    entities[qid]['name'] = n
    entities[qid]['freq'] = f
    entities[qid]['desc'] = d
    entities[qid]['aka'] = a.split(' , ')
  return entities


def dump_as_json(entities, file_path, as_jsonlines=True):
  if as_jsonlines:
    with open(file_path, 'a') as f:
      for entity in entities.values():
        json.dump(entity, f, ensure_ascii=False)
        f.write('\n')
  else:
    with open(file_path, 'w') as f:
      json.dump(entities, f, indent=4, ensure_ascii=False)

  
@timewatch
def main(args):
  # Process Properties.
  source_filepath = args.source_dir + '/properties.bin'
  text_filepath = args.source_dir + '/properties.txt'
  target_filepath = args.source_dir + '/properties.tokenized.jsonlines'
  if not os.path.exists(target_filepath) or args.cleanup:
    #props = pickle.load(open(source_filepath, 'rb'))
    props = output_and_parse(source_filepath, text_filepath)
    dump_as_json(props, target_filepath, as_jsonlines=True)
    target_filepath = args.source_dir + '/properties.tokenized.json'
    dump_as_json(props, target_filepath, as_jsonlines=False)

  #exit(1)
  source_filepath = args.source_dir + '/items.bin'
  text_filepath = args.source_dir + '/items.txt'
  target_filepath = args.source_dir + '/items.tokenized.jsonlines'
  # Process Items.
  if not os.path.exists(target_filepath) or args.cleanup:
    #items = pickle.load(open(source_filepath, 'rb'))
    items = output_and_parse(source_filepath, text_filepath)
    dump_as_json(items, target_filepath, as_jsonlines=True)
    target_filepath = args.source_dir + '/items.tokenized.json'
    dump_as_json(items, target_filepath, as_jsonlines=False)




if __name__ == "__main__":
  desc = 'Tokenize items.bin and properties.bin in args.source_dir and store the tokenized files there.'
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', default='wikidata/latest/extracted/all')
  parser.add_argument('--cleanup', default=False, type=str2bool)
  args = parser.parse_args()

  main(args)
