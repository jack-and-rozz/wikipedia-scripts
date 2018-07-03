# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict
from getpass import getpass
import multiprocessing as mp
import argparse, sys, os, time, json, subprocess, re, itertools, random, itertools, codecs
from common import str2bool, timewatch, multi_process

try:
   import pickle as pickle
except:
   import pickle

RED = "\033[31m"
BLACK = "\033[30m"
UNDERLINE = '\033[4m'
BOLD = "\033[1m" + UNDERLINE

RESET = "\033[0m"

LRB = '-LRB-' # (
RRB = '-RRB-' # )
LSB = '-LSB-' # [
RSB = '-RSB-' # ]
LCB = '-LCB-' # {
RCB = '-RCB-' # }

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
  para = [" ".join([w[0] for w in sent['words']]) for sent in sentences]
  return para

############################################
##              Main
############################################

@timewatch
def read_db(target_dir, dbuser, dbpass, dbhost, dbname):
  # page.sql, wbc_entity_usage.sql, redirect.sql need to be loaded into MySQL in advance.
  if os.path.exists(target_dir + '/title2qid.bin'): 
    sys.stderr.write('Found \'title2qid.bin\' ...\n')
    title2qid = pickle.load(open(target_dir + '/title2qid.bin', 'rb'))
  else:
    import MySQLdb
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
    #sql = "select page.page_title, wbc.eu_entity_id from page inner join wbc_entity_usage as wbc on page.page_id = wbc.eu_page_id where page.page_namespace=0 and page.page_is_redirect=0 and wbc.eu_aspect='S';"
    sql = "select page.page_title, wbc.eu_entity_id from page inner join wbc_entity_usage as wbc on page.page_id = wbc.eu_page_id where page.page_namespace=0 and page.page_is_redirect=0 and wbc.eu_aspect='S';"
    c.execute(sql)
    title2qid = {}
    for row in c.fetchall():
      title, qid = row
      # Byte to str
      title = title.decode('utf-8')
      qid = qid.decode('utf-8')
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
  sys.stderr.write('Finish loading \'title2qid.bin\' ...\n')
  
  return title2qid

def color_link(text, link_spans):
  text = text.split()
  for _, start, end in link_spans:
    text[start] = RED + text[start]
    text[end] = text[end] + RESET
  return ' '.join(text)


def process_sentence(original_sent, titles):
  # Fix the prural word splitted by the link (e.g. [[...|church]]es. ).
  sent = original_sent
  sent = ' '.join([w for w in sent.split() if w])
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
  sent = re.sub('\\\/', '/', sent)
  sent = re.sub('([;\,\/] ){2,}', ', ', sent)
  sent = re.sub('%s\s*%s ' % (LRB, RRB), '', sent)
  sent = ' '.join([w for w in sent.split() if w])
  
  # get link spans
  link_idx = [j for j, w in enumerate(sent.split()) if w == SYMLINK]
  for i, idx in enumerate(link_idx):
    title = titles[i]
    start = idx + sum([len(p.split()) - 1 for p in link_phrases[:i]])
    end = start + len(link_phrases[i].split()) - 1
    link_spans.append((title, start, end))
  sent = sent.split()
  for i, idx in enumerate(link_idx):
    sent[idx] = link_phrases[i]
  sent = ' '.join(sent).replace(LRB, '(').replace(RRB, ')').replace(LSB, '[').replace(RSB, ']').replace(LCB, '{').replace(RCB, '}')

  link_spans = [(title2qid[t], s, e) for t,s,e in link_spans if t in title2qid]
  return sent, link_spans


def to_title_format(title_str):
  res = title_str.replace(' ', '_')
  return res[0].upper() + res[1:]

def process_paragraph(pid, ptitle, para_idx, paragraph, s_parser):
  para = origin = paragraph 

  # stanford's sentence splitter doesn't always work well around the brackers.
  # e,g, "A are one of the [[...|singular]]s. B are ...". (Not to be splitted)
  #m = re.search('\]\](\S+?)\. ', para)
  #for m in re.findall('\]\]([A-Za-z0-9]+?)', para):

  # todo:短くしないほうが良いか？要確認
  for m in re.findall('\]\]([A-Za-z0-9]+)', para):
    para = para.replace(']]' + m, m + ']] ')

  # Remove phrases enclosed in parentheses.
  # (Those are usually expressions in different languages, or acronyms.)

  #link_template = '\[\[[^\[\]]+?(\(.+?\)).*?\|([^\[\]]+?)\]\]'
  #linked_parantheses = [m2[0] for m2 in re.findall(link_template, para)]
  #_linked_parantheses = [m2[1] for m2 in re.findall(link_template, para)]
  for m in re.findall('\([\S\s]*?\)', para):
    # if m not in linked_parantheses:
    para = para.replace(m , '')

  # Get precise titles from link template before parsing.
  # (if after, e.g., 'CP/M-86' can be splited into 'CP/M -86' in tokenizing and become a wrong title.)
  link_template = '\[\[(.+?)\|(.+?)\]\]'

  ltitles = [to_title_format(m[0]) for m in re.findall(link_template, para)]
  # Tokenize by stanford parser.
  para = stanford_tokenizer(para, s_parser)
  if args.n_sentence:
    para = para[:args.n_sentence]

  results = []
  for s in para:
    n_detected_titles = sum([len(ls) for (_, ls) in results])
    sent, link_spans = process_sentence(s, ltitles[n_detected_titles:])
    results.append((sent, link_spans))

  if args.debug:
    #idx = "%s (%s-%d)" % (ptitle, pid, para_idx)
    qid = title2qid[ptitle] if ptitle in title2qid else 'None'
    idx = "%s (%s:%s-%d)" % (ptitle, qid, pid, para_idx)
    sys.stdout.write("%s%s Original%s: %s\n" % (BOLD, idx, RESET, origin))
    for sent_idx, (sent, link_spans) in enumerate(results):
      #idx = "%s (%s-%d-%d)" % (ptitle, pid, para_idx, sent_idx)
      idx = "%s (%s:%s-%d-%d)" % (ptitle, qid, pid, para_idx, sent_idx)
      sys.stdout.write("%s%s Processed%s: %s\n"  % (
        BOLD, idx, RESET, color_link(sent, link_spans)))
      sys.stdout.write("%s%s Links%s: %s\n" % (
        BOLD, idx, RESET, link_spans))
      sys.stdout.write("\n")


  return results  # res[sentence_idx] = (text, link_spans)

def process_page(page, s_parser):
  pid = page['pid']
  text = page['text']
  title = page['title']
  paragraphs = [line for line in text.split('\n') if len(line) > 0 and line != '　']

  # Use the second paragraph (the first paragraph is the title)
  if len(paragraphs) <= 1:
    return None
  res = []
  paragraphs = paragraphs[1:1+args.n_paragraph] if args.n_paragraph else paragraphs[1:]
  
  for para_idx, para in enumerate(paragraphs):
    res_paragraph = process_paragraph(pid, title, para_idx, para, s_parser)

    # Include paragraphs that include no links for now (the context may be used for something).
    res.append(res_paragraph)
  return res   # res[paragraph_id][sentence_idx] = (text, link_spans)

#@timewatch
def read_json_lines(source_path):
  if args.debug:
    sys.stdout.write(source_path + '\n')
  s_parser = corenlp.StanfordCoreNLP(corenlp_path=corenlp_dir,
                                     properties=properties_file)
  res = OrderedDict()
  with open(source_path) as f:
    for i, page in enumerate(f):
      j = json.loads(page)
      pid = j['id']
      text = j['text']
      ptitle = to_title_format(j['title'])
      page_qid = title2qid[ptitle] if ptitle in title2qid else None
      if not page_qid:
        continue
      page = {
        'title': ptitle,
        'pid': pid,
        'qid': page_qid,
        'text': text,
      }
      page_res = process_page(page, s_parser)

      # Discard articles that contain no links.
      if not page_res:
        continue

      # Arrange page_res.
      page_text = []
      page_links = OrderedDict()
      for i, para in enumerate(page_res):
        para_text = []
        para_links = OrderedDict()
        for j, (sent, links) in enumerate(para):
          para_text.append(sent)
          for link_qid, s, t in links:
            para_links[link_qid] = (i, j, (s, t))
        page_text.append(para_text)
        page_links.update(para_links)
      res[page_qid] = {
        'title': page['title'],
        'pid': page['pid'],
        'qid': page['qid'],
        'text': page_text,
        'link': page_links,
      }
  return res

def read_all_pages():
  sys.stderr.write('Reading articles ...\n')
  all_pathes = str(subprocess.getoutput('ls -d %s/*/wiki_*' % args.source_dir)).split()
  if args.max_wikifiles:
    all_pathes = all_pathes[:args.max_wikifiles]

  sys.stderr.write("Numbers of json files: %d \n" % len(all_pathes))
  #sys.stderr.write("%s\n" % str([re.search('/(.+?/wiki_[0-9]+)', p).group(1) for p in all_pathes]))

  res = OrderedDict({})
  count = 1
  n_finished_files = 0
  for _, pathes in itertools.groupby(enumerate(all_pathes), lambda x: x[0] // (args.n_process)):
    pathes = [p[1] for p in pathes]
    res_process = multi_process(read_json_lines, pathes)
    n_finished_files += len(pathes)
    for r in res_process:
      res.update(r)
    if len(res) > count * 500000:
      count += 1
      sys.stderr.write("Finish reading %d/%d files (%d articles) ...\n" % (n_finished_files, len(all_pathes), len(res)))

  sys.stderr.write("Finish reading %d/%d files (%d articles) ...\n" % (n_finished_files, len(all_pathes), len(res)))
  return res

def only_linked(pages):
  new_pages = OrderedDict()
  for pid, page in list(pages.items()):
    # only linked sentences
    new_page = [[(sent, link_spans) for sent, link_spans in para if link_spans] for para in page]
    # only linked paragraphs
    new_page = [para for para in new_page if para]
    if new_page:
      new_pages[pid] = new_page 
  return new_pages

def count(pages):
  n_articles = len(pages) 
  n_paragraph = sum([len(page['text']) for page in pages.values()])
  n_sentence = sum([sum([len(para) for para in page['text']]) for page in pages.values()])
  n_links = sum([len(page['link']) for page in pages.values()])
  sys.stdout.write("\n")
  sys.stdout.write("Number of Articles: %d \n" % n_articles)
  sys.stdout.write("Number of Paragraphs: %d \n" % n_paragraph)
  sys.stdout.write("Number of Sentences: %d \n" % n_sentence)
  sys.stdout.write("Number of Links: %d \n" % n_links)


@timewatch
def create_dump(args):
  output_dir = args.output_dir
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)
  # Create json dumps.
  output_file = 'pages.all.json'
  output_path = os.path.join(output_dir, output_file)
  if not os.path.exists(output_path) or not args.cleanup:
    global title2qid
    title2qid = read_db(args.source_dir, args.dbuser, args.dbpass, 
                        args.dbhost, args.dbname)
    pages = read_all_pages()
  else:
    sys.stderr.write('Found dump files. \n') 
    return

  # create a json dump.
  with open(output_path, 'w') as f:
    json.dump(pages, f, indent=4, ensure_ascii=False,)

  with open(output_path + 'lines', 'a') as f:
    for page in pages.values():
      json.dump(page, f, ensure_ascii=False)
      f.write('\n')

  # Create pickle dumps.
  output_file = 'pages.all.bin'
  output_path = os.path.join(output_dir, output_file)
  pickle.dump(pages, open(output_path, 'wb'))
  sys.stderr.write('<All> \n')
  count(pages)
  return 


  # Create dumps only of linked sentences.
  output_file = 'pages.only_linked.bin'
  output_path = os.path.join(output_dir, output_file)
  selected_pages = only_linked(pages)
  pickle.dump(selected_pages, open(output_path, 'wb'))
  sys.stderr.write('<Selected> \n')
  count(selected_pages)


  # Create pickle dumps as small chunks.
  output_file = 'pages.bin'
  output_path = os.path.join(output_dir, output_file)
  chunk_size = 100000
  if args.cleanup or not str(subprocess.getoutput('ls -d %s/* | grep *\.bin.[0-9]\+' % output_dir)).split():
    for i, d in itertools.groupby(enumerate(pages), lambda x: x[0] // chunk_size):
      chunk = {x[1]:pages[x[1]] for x in d}
      pickle.dump(chunk, open(output_path + '.%02d' %i, 'wb'))

  return pages


@timewatch
def main(args):
  pages = create_dump(args)

if __name__ == "__main__":
  desc = "This script creates wikiP2D corpus from Wikipedia dump sqls (page.sql, wbc_entity_usage.sql) and a xml file (pages-articles.xml) parsed by WikiExtractor.py (https://github.com/attardi/wikiextractor.git) with '--filter_disambig_pages --json' options."
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('-o', '--output_dir', default='wikipedia/latest/extracted/tmp')
  parser.add_argument('-s', '--source_dir', default='wikipedia/latest/extracted', 
                      help='the directory of wiki_** files parsed by WikiExtractor.py from enwiki-***-pages-articles.xml')
  parser.add_argument('-mw', '--max_wikifiles', default=0, type=int)
  parser.add_argument('-npr','--n_process', default=1, type=int)
  parser.add_argument('-npg','--n_paragraph', default=1, type=int, help='if None, this script reads all paragraphs in the paragraph.')
  parser.add_argument('-nst','--n_sentence', default=0, type=int, help='if None, this script reads all sentences in the paragraph.')

  parser.add_argument('--dbuser', default='shoetsu')
  parser.add_argument('--dbpass', default='password')
  parser.add_argument('--dbhost', default='localhost')
  parser.add_argument('--dbname', default='wikipedia')
  parser.add_argument('--debug', default=True, type=str2bool)
  parser.add_argument('--cleanup', default=False, type=str2bool)

  args = parser.parse_args()
  main(args)

# sapporo
# nohup python wp_extract_all.py -o wikipedia/latest/extracted/dumps.p1s0 -mw 0 -npr 16 -npg 1 -nst 0 > logs/dumps.p1s0.log 2> logs/dumps.p1s0.err&
