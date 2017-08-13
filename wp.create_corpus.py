# coding: utf-8

from collections import OrderedDict
from getpass import getpass
import MySQLdb, argparse, sys, os, time, json, commands, re, itertools, random

import multiprocessing as mp
from nltk.parse import stanford

try:
   import cPickle as pickle
except:
   import pickle

RED = "\033[31m"
BLACK = "\033[30m"
UNDERLINE = '\033[4m'
BOLD = "\033[1m" + UNDERLINE

RESET = "\033[0m"

LRB = '-LRB-'
LSB = '-LSB-'
RRB = '-RRB-'
RSB = '-RSB-'

# It seems difficult to use this in multiprocessing....
import corenlp
corenlp_dir = os.path.join(os.environ['HOME'], 
                           "workspace/downloads/stanford-corenlp")
properties_file = os.path.join(corenlp_dir, 'user.properties')


def stanford_tokenizer(sentence, s_parser):
  result = json.loads(s_parser.parse(sentence))
  # Use only the first sentence. 
  sent = " ".join([w[0] for w in result['sentences'][0]['words']]).encode('utf-8')
  return sent


def stanford_tokenizer_cmd(sentence):
  # todo:
  tmp_dir = '/tmp'
  file_path = os.path.join(tmp_dir, 'tokenizing%d.tmp' % random.randint(0, 10000000))
  while os.path.exists(file_path):
    file_path = os.path.join(tmp_dir, 'tokenizing%d.tmp' % random.randint(0, 10000000))
  with open(file_path, 'w') as f:
    f.write(sentence + '\n')
  cmd='java edu.stanford.nlp.process.PTBTokenizer -preserveLines < %s 2> /dev/null' % file_path
  tokenized = commands.getoutput(cmd).split('\n')
  os.system("rm %s" % file_path)
  return tokenized[0]

def str2bool(v):
  if type(v) == bool:
    return v
  return v.lower() in ("yes", "true", "t", "1")

def timewatch(func):
  def wrapper(*args, **kwargs):
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    print (func.__name__, ": ", end - start)
    return result
  return wrapper

@timewatch
def read_db(target_dir, dbhost, dbname):
  if os.path.exists(target_dir + '/title2qid.bin'):
    title2qid = pickle.load(open(target_dir + '/title2qid.bin'))
  else:
    sys.stdout.write('DB Username: ')
    #dbuser = raw_input()
    sys.stdout.write('DB Password: ')
    #dbpass = getpass()

    dbuser='shoetsu'
    dbpass='password'
    conn = MySQLdb.connect(
      user=dbuser,
      passwd=dbpass,
      host=dbhost,
      db=dbname
    )
    c = conn.cursor()
    sql = "select page.page_id, page.page_title, wbc.eu_entity_id from page inner join wbc_entity_usage as wbc on page.page_id = wbc.eu_page_id where page.page_namespace=0 and page.page_is_redirect=0 and wbc.eu_aspect='S';"
    c.execute(sql)
    title2qid = {}
    for row in c.fetchall():
      pid, title, qid = row
      pid = str(pid)
      title2qid[title] = qid
    pickle.dump(title2qid, open(target_dir + '/title2qid.bin', 'wb'))
  return title2qid

def color_link(text, link_spans):
  text = text.split(' ')
  for _, start, end in link_spans:
    text[start] = RED + text[start]
    text[end] = text[end] + RESET
  return ' '.join(text)



def process_page(page, s_parser):
  pid = page['pid']
  text = page['text']

  paragraphs = [line for line in text.split('\n') if len(line) > 0 and line != '　']

  # Use the second paragraph (the first paragraph is the title)
  if len(paragraphs) <= 1:
    return None
  para = paragraphs[1]
  origin = para

  # stanford's sentence splitter doesn't always work well around the brackers.
  # e,g, "A are one of the [[...|singular]]s. B are ...". (Not to be splitted)
  #m = re.search('\]\](\S+?)\. ', para)
  for m in re.findall('(\]\]\S+?)\. ', para):
    para = para.replace(m, m + ' ')


  # Remove phrases enclosed in parentheses with no links.
  # (Those are usually expressions in different languages, or acronyms.)
  for m in re.findall('\([\S\s]+?\)', para):
    # Dont remove enclosed phrases that are part of the title of an entity.
    expr = '(\[\[[^\[]+?)%s([^\]]+?)\]\]' % m
    if not re.search(expr, para):
      para = para.replace(m , '')


  # Get precise titles from link template before parsing.
  # (if after, e.g., 'CP/M-86' can be splited into 'CP/M -86' in tokenizing and become a wrong title)

  link_template = '\[\[(.+?)\|(.+?)\]\]'
  titles = [m[0].replace(' ', '_') for m in re.findall(link_template, para)]
  if pid == '2215':
    pass

  # Tokenize by stanford parser.
  para = stanford_tokenizer(para, s_parser)

  # Fix the prural word splitted by the link (e.g. [[...|church]]es. ).
  for m in set(re.findall(' %s %s (e?s) ' % (RSB, RSB), para)):
    para = para.replace(' %s %s %s' % (RSB, RSB, m),
                        '%s %s %s' % (m, RSB, RSB,),)

  link_phrases = []
  link_spans = []

  # replace link expressions [[wiki_title | raw_phrase]] to @LINK.
  SYMLINK = '@LINK'
  link_template = '%s %s (.+?) \| (.+?) %s %s' % (LSB, LSB, RSB, RSB)

  for i, m in enumerate(re.finditer(link_template, para)):
    link, _, link_phrase = m.group(0), m.group(1), m.group(2)
    link_phrases.append(link_phrase)
    para = para.replace(link, SYMLINK)
    # We use only linked pages.
    if len(link_phrases) == 0:
      return "", []

  # Remove continuous delimiters, etc.
  # (caused by removing external links when the xml file was parsed).
  # para = re.sub('[\,;]\s?%s' % (RRB), RRB, para)
  # para = re.sub('%s\s?[\,;]' % (LRB), LRB, para)
  para = re.sub('\\\/', '/', para)
  para = re.sub('([;\,\/] ){2,}', ', ', para)
  para = re.sub('%s\s*%s ' % (LRB, RRB), '', para)

  # para = re.sub('%s ([,;] )+' % (LRB), LRB, para)
  # para = para.replace('%s\s*%s ' % (LRB, RRB), '')


  # get link spans
  link_idx = [j for j, w in enumerate(para.split(' ')) if w == SYMLINK]
  for i, idx in enumerate(link_idx):
    title = titles[i]
    start = idx + sum([len(p.split(' '))-1 for p in link_phrases[:i]])
    end = start + len(link_phrases[i].split(' ')) - 1
    link_spans.append((title, start, end))
  para = para.split(' ')
  for i, idx in enumerate(link_idx):
    para[idx] = link_phrases[i]
  para = ' '.join(para).replace(LRB, '(').replace(RRB, ')')

  if args.debug:
    pass
    sys.stderr.write("%s(%s) Original text%s: %s\n" % (BOLD, pid, RESET, origin))

    sys.stderr.write("%s(%s) Processed text%s: %s\n"  % (
      BOLD, pid, RESET, color_link(para, link_spans)))
    sys.stderr.write("%s(%s) Link spans%s: %s\n" % (
      BOLD, pid, RESET, link_spans))
    sys.stderr.write("\n")
    
    #res.append((para, link_spans))
  return (para, link_spans)

@timewatch
def read_json(source_path, title2qid, q=None):
  if args.debug:
    sys.stderr.write(source_path + '\n')
  s_parser = corenlp.StanfordCoreNLP(corenlp_path=corenlp_dir,
                                     properties=properties_file)

  res = OrderedDict()
  with open(source_path) as f:
    for i, page in enumerate(f):
      j = json.loads(page)
      pid = j['id'].encode('utf-8')
      text = j['text'].encode('utf-8')
      title = j['title'].encode('utf-8').replace(' ', '_')
      # if title not in title2qid:
      #   continue
      # qid = title2qid[title]
      qid = title
      page = {
        'title': title,
        'pid': pid,
        'qid': qid,
        'text': text,
      }
      sent, link_spans = process_page(page, s_parser)
      if not link_spans:
        continue
      # 'plain_text, [(title, start, end), ...]'
      res[qid] = (sent, link_spans)
  if q:
    q.put(res)

def multi_read_json(source_pathes, title2qid):
  workers = []
  # mp.Queue() seems to have a bug..? 
  # (stackoverflow.com/questions/13649625/multiprocessing-in-python-blocked)
  q = mp.Manager().Queue() 
  for path in source_pathes:
    worker = mp.Process(target=read_json, args=(path, title2qid, q))
    workers.append(worker)
    worker.daemon = True  # make interrupting the process with ctrl+c easier
    worker.start()

  
  for worker in workers:
    worker.join()
  results = []
  while not q.empty():
    res = q.get()
    results.append(res)
  return results

@timewatch
def main(args):
  sys.stderr.write('Reading wikipedia DB...\n')
  #title2qid = read_db(args.source_dir, args.dbhost, args.dbname)
  title2qid=None
  sys.stderr.write('Reading articles ...\n')
  all_pathes = commands.getoutput('ls -d %s/*/wiki_*' % args.source_dir).split()
  for _, pathes in itertools.groupby(enumerate(all_pathes), lambda x: x[0] // (args.n_process)):
    pathes = [p[1] for p in pathes]
    #pathes = [pathes[1]]
    res = multi_read_json(pathes, title2qid)
    print sum([len(r) for r in res])
    #articles = read_json(source_path, title2qid)
    break
  pass

if __name__ == "__main__":
  desc = "This script creates wikiP2D corpus from Wikipedia dump sqls (page.sql, wbc_entity_usage.sql) and a xml file (pages-articles.xml) parsed by WikiExtractor.py (https://github.com/attardi/wikiextractor.git) with '--filter_disambig_pages --json' options."
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', 
                      default='/home/shoetsu/disk/dataset/wikipedia/en/latest/extracted')
  parser.add_argument('--dbhost', default='localhost')
  parser.add_argument('--dbname', default='wikipedia')
  parser.add_argument('--debug', default=True, type=str2bool)
  parser.add_argument('--n_process', default=1, type=int)
  args = parser.parse_args()
  main(args)
