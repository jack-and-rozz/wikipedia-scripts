# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict
from getpass import getpass
import MySQLdb
import multiprocessing as mp
import argparse, sys, os, time, json, commands, re, itertools, random, itertools
from common import str2bool, timewatch, multi_process

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

import corenlp
corenlp_dir = os.path.join(os.environ['HOME'], 
                           "workspace/downloads/stanford-corenlp")
properties_file = os.path.join(corenlp_dir, 'user.properties')

############################################
##            Tokenizer
############################################

def stanford_tokenizer(text, s_parser):
  result = json.loads(s_parser.parse(text))
  sentences = [sent for sent in result['sentences'] if sent['words']]
  para = [" ".join([w[0] for w in sent['words']]).encode('utf-8') for sent in sentences]
  return para

############################################
##              Main
############################################

@timewatch
def read_db(target_dir, dbuser, dbpass, dbhost, dbname):
  # page.sql, wbc_entity_usage.sql, redirect.sql need to be loaded into MySQL in advance.
  if os.path.exists(target_dir + '/title2qid.bin'): 
    sys.stderr.write('Found \'title2qid.bin\' ...\n')
    title2qid = pickle.load(open(target_dir + '/title2qid.bin'))
  else:
    sys.stderr.write('Reading wikipedia DB...\n')
    conn = MySQLdb.connect(
      user=dbuser,
      passwd=dbpass,
      host=dbhost,
      db=dbname)
    c = conn.cursor()

    # <Memo>
    # PageとRedirectのnamespaceはとりあえず両方0のみでok (https://en.wikipedia.org/wiki/Wikipedia:Namespace)
    # wbc.eu_aspectについては多分 'S' だけでok (namespace=0ならほぼ常に'S'は存在し、重複もしない) (https://www.mediawiki.org/wiki/Wikibase/Schema/wbc_entity_usage)
    # (All, T, X, S, O) = (8228959, 1401357, 1619537, 3374147, 1294316)

    # Get pages joined with wbc.eu_entity_id (wikidata ID).
    sql = "select page.page_title, wbc.eu_entity_id from page inner join wbc_entity_usage as wbc on page.page_id = wbc.eu_page_id where page.page_namespace=0 and page.page_is_redirect=0 and wbc.eu_aspect='S';"
    c.execute(sql)
    title2qid = {}
    for row in c.fetchall():
      title, qid = row
      title2qid[title] = qid
    sys.stderr.write("Numbers of titles with entity-id: %d \n" % len(title2qid))

    # Add redirects. 
    sql = "select page.page_title, rd.rd_title from page inner join redirect as rd on page.page_id = rd.rd_from where page.page_namespace=0 and rd.rd_namespace=0;"
    c.execute(sql)
    for row in c.fetchall():
      from_title, to_title = row
      if to_title in title2qid:
        qid = title2qid[to_title]
        title2qid[from_title] = qid
    sys.stderr.write("Numbers of titles with entity-id (after redirects added): %d \n" % len(title2qid))
    c.close()
    conn.close()
    pickle.dump(title2qid, open(target_dir + '/title2qid.bin', 'wb'))
  return title2qid

def color_link(text, link_spans):
  text = text.split(' ')
  for _, start, end in link_spans:
    text[start] = RED + text[start]
    text[end] = text[end] + RESET
  return ' '.join(text)


def process_sentence(sent, titles):
  # Fix the prural word splitted by the link (e.g. [[...|church]]es. ).
  for m in set(re.findall(' %s %s (e?s) ' % (RSB, RSB), sent)):
    sent = sent.replace(' %s %s %s' % (RSB, RSB, m),
                        '%s %s %s' % (m, RSB, RSB,),)

  link_phrases = []
  link_spans = []

  # replace link expressions [[wiki_title | raw_phrase]] to @LINK.
  SYMLINK = '@LINK'
  link_template = '%s %s (.+?) \| (.+?) %s %s' % (LSB, LSB, RSB, RSB)

  for i, m in enumerate(re.finditer(link_template, sent)):
    link, _, link_phrase = m.group(0), m.group(1), m.group(2)
    link_phrases.append(link_phrase)
    sent = sent.replace(link, SYMLINK)

  # Remove continuous delimiters, etc.
  # (caused by removing external links when the xml file was parsed).
  # sent = re.sub('[\,;]\s?%s' % (RRB), RRB, sent)
  # sent = re.sub('%s\s?[\,;]' % (LRB), LRB, sent)
  sent = re.sub('\\\/', '/', sent)
  sent = re.sub('([;\,\/] ){2,}', ', ', sent)
  sent = re.sub('%s\s*%s ' % (LRB, RRB), '', sent)

  # get link spans
  link_idx = [j for j, w in enumerate(sent.split(' ')) if w == SYMLINK]
  for i, idx in enumerate(link_idx):
    title = titles[i]
    start = idx + sum([len(p.split(' ')) - 1 for p in link_phrases[:i]])
    end = start + len(link_phrases[i].split(' ')) - 1
    link_spans.append((title, start, end))
  sent = sent.split(' ')
  for i, idx in enumerate(link_idx):
    sent[idx] = link_phrases[i]
  sent = ' '.join(sent).replace(LRB, '(').replace(RRB, ')')

  link_spans = [(title2qid[t], s, e) for t,s,e in link_spans if t in title2qid]
  return sent, link_spans


def to_title_format(title_str):
  res = title_str.replace(' ', '_')
  return res[0].upper() + res[1:]

def process_paragraph(pid, para_idx, paragraph, s_parser):
  para = origin = paragraph 

  # stanford's sentence splitter doesn't always work well around the brackers.
  # e,g, "A are one of the [[...|singular]]s. B are ...". (Not to be splitted)
  #m = re.search('\]\](\S+?)\. ', para)
  for m in re.findall('\]\]([A-Za-z0-9]+?)', para):
    para = para.replace(']]' + m, m + ']] ')

  # Remove phrases enclosed in parentheses with no links.
  # (Those are usually expressions in different languages, or acronyms.)
  link_template = '\[\[[^\[\]]+?(\(.+?\)).*?\|([^\[\]]+?)\]\]'
  linked_parantheses = [m2[0] for m2 in re.findall(link_template, para)]
  _linked_parantheses = [m2[1] for m2 in re.findall(link_template, para)]
  for m in re.findall('\([\S\s]*?\)', para):
    if m not in linked_parantheses:
       para = para.replace(m , '')

  # Get precise titles from link template before parsing.
  # (if after, e.g., 'CP/M-86' can be splited into 'CP/M -86' in tokenizing and become a wrong title.)
  link_template = '\[\[(.+?)\|(.+?)\]\]'

  titles = [to_title_format(m[0]) for m in re.findall(link_template, para)]
  # Tokenize by stanford parser.
  para = stanford_tokenizer(para, s_parser)
  if args.n_sentence:
    para = para[:args.n_sentence]

  results = []
  for s in para:
    n_detected_titles = sum([len(ls) for (_, ls) in results])
    sent, link_spans = process_sentence(s, titles[n_detected_titles:])
    results.append((sent, link_spans))

  if args.debug:
    idx = "%s-%d" % (pid, para_idx)
    sys.stdout.write("%s(%s) Original text%s: %s\n" % (BOLD, idx, RESET, origin))
    for sent_idx, (sent, link_spans) in enumerate(results):
      idx = "%s-%d-%d" % (pid, para_idx, sent_idx)
      sys.stdout.write("%s(%s) Processed text%s: %s\n"  % (
        BOLD, idx, RESET, color_link(sent, link_spans)))
      sys.stdout.write("%s(%s) Link spans%s: %s\n" % (
        BOLD, idx, RESET, link_spans))
      sys.stdout.write("\n")
  # Include sentences that include no links for now (the context may be used for something).
  # results = [r for r in results if len(r[1]) > 0 ]
  return results

def process_page(page, s_parser):
  # Returns a 2nd order tensor.
  # (tensor[paragraph_id][sentence_idx] = (text, link_spans))
  pid = page['pid']
  text = page['text']

  paragraphs = [line for line in text.split('\n') if len(line) > 0 and line != '　']

  # Use the second paragraph (the first paragraph is the title)
  if len(paragraphs) <= 1:
    return None
  res = []
  paragraphs = paragraphs[1:1+args.n_paragraph] if args.n_paragraph else paragraphs[1:]
  
  for para_idx, para in enumerate(paragraphs):
    res_paragraph = process_paragraph(pid, para_idx, para, s_parser)

    # Include paragraphs that include no links for now (the context may be used for something).
    #if res_paragraph:
    res.append(res_paragraph)
  return res

#@timewatch
def read_json_lines(source_path):
  #
  if args.debug:
    sys.stdout.write(source_path + '\n')
  s_parser = corenlp.StanfordCoreNLP(corenlp_path=corenlp_dir,
                                     properties=properties_file)
  res = OrderedDict()
  with open(source_path) as f:
    for i, page in enumerate(f):
      j = json.loads(page)
      pid = j['id'].encode('utf-8')
      text = j['text'].encode('utf-8')
      title = to_title_format(j['title'].encode('utf-8'))
      page = {
        'title': title,
        'pid': pid,
        #'qid': qid,
        'text': text,
      }
      page_res = process_page(page, s_parser)

      # Discard articles that contain no links.
      if not page_res:
        continue
      # 'plain_text, [(title, start, end), ...]'
      #res[title2qid[title]] = page_res
      res[pid] = page_res
  return res

def read_all_pages():
  sys.stderr.write('Reading articles ...\n')
  all_pathes = commands.getoutput('ls -d %s/*/wiki_*' % args.source_dir).split()
  sys.stderr.write("Numbers of json files: %d \n" % len(all_pathes))

  res = OrderedDict({})
  count = 1
  for _, pathes in itertools.groupby(enumerate(all_pathes), lambda x: x[0] // (args.n_process)):
    pathes = [p[1] for p in pathes]
    res_process = multi_process(read_json_lines, pathes)
    for r in res_process:
      res.update(r)
    #return res
    if len(res) > count * 500000:
      count += 1
      sys.stderr.write("Finish reading %d articles ...\n" % len(res))

  sys.stderr.write("Finish reading %d articles ...\n" % len(res))
  return res

def only_linked(pages):
  new_pages = OrderedDict()
  for pid, page in pages.items():
    # only linked sentences
    new_page = [[(sent, link_spans) for sent, link_spans in para if link_spans] for para in page]
    # only linked paragraphs
    new_page = [para for para in new_page if para]
    if new_page:
      new_pages[pid] = new_page 
  return new_pages

def count(pages):
  n_articles = len(pages) 
  n_paragraph = sum((len(page) for page in pages.values()))
  n_sentence = sum((sum((len(para) for para in page)) for page in pages.values()))
  n_links = sum((sum((sum((len(sent[1]) for sent in para)) for para in page)) for page in pages.values()))
  sys.stderr.write("Number of Articles: %d \n" % n_articles)
  sys.stderr.write("Number of Paragraphs: %d \n" % n_paragraph)
  sys.stderr.write("Number of Sentences: %d \n" % n_sentence)
  sys.stderr.write("Number of Links: %d \n" % n_links)


@timewatch
def create_dump(args):
  output_suffix = '.p%ds%d' % (args.n_paragraph, args.n_sentence)
  dump_dir = args.source_dir + '/dumps' + output_suffix
  if not os.path.exists(dump_dir):
    os.makedirs(dump_dir)

  output_file = '/pages%s.all.bin' % (output_suffix)
  if os.path.exists(dump_dir + output_file) and not args.cleanup:
    sys.stderr.write('Found pages.*.bin file ...\n')
    pages = pickle.load(open(dump_dir + output_file, 'rb'))
  else:
    global title2qid
    title2qid = read_db(args.source_dir, args.dbuser, args.dbpass, 
                        args.dbhost, args.dbname)
    pages = read_all_pages()
    pickle.dump(pages, open(dump_dir + output_file, 'wb'))

  output_file = '/pages%s.bin' % (output_suffix)
  if os.path.exists(dump_dir + output_file) and not args.cleanup:
    selected_pages = pickle.load(open(dump_dir + output_file, 'rb'))
  else:
    selected_pages = only_linked(pages)
    pickle.dump(selected_pages, open(dump_dir + output_file, 'wb'))

  sys.stderr.write('<All> \n')
  count(pages)
  sys.stderr.write('<Selected> \n')
  count(selected_pages)

  chunk_size = 100000
  if args.cleanup or not commands.getoutput('ls -d %s/* | grep *\.bin.[0-9]\+' % dump_dir).split():
    for i, d in itertools.groupby(enumerate(pages), lambda x: x[0] // chunk_size):
      chunk = {x[1]:pages[x[1]] for x in d}
      pickle.dump(chunk, open(dump_dir + output_file + '.%d' %i, 'wb'))
  return pages


@timewatch
def main(args):
  pages = create_dump(args)

if __name__ == "__main__":
  desc = "This script creates wikiP2D corpus from Wikipedia dump sqls (page.sql, wbc_entity_usage.sql) and a xml file (pages-articles.xml) parsed by WikiExtractor.py (https://github.com/attardi/wikiextractor.git) with '--filter_disambig_pages --json' options."
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--source_dir', default='wikipedia/latest/extracted', 
                      help='the directory of wiki_** files parsed by WikiExtractor.py from enwiki-***-pages-articles.xml')
  parser.add_argument('--dbuser', default='shoetsu')
  parser.add_argument('--dbpass', default='password')
  parser.add_argument('--dbhost', default='localhost')
  parser.add_argument('--dbname', default='wikipedia')
  parser.add_argument('--debug', default=True, type=str2bool)

  parser.add_argument('--n_process', default=1, type=int)
  parser.add_argument('--n_paragraph', default=1, type=int, help='if None, this script reads all paragraphs in the paragraph.')
  parser.add_argument('--n_sentence', default=1, type=int, help='if None, this script reads all sentences in the paragraph.')
  parser.add_argument('--cleanup', default=False, type=str2bool)

  args = parser.parse_args()
  main(args)
