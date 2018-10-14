# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict
from getpass import getpass
from tqdm import tqdm

import multiprocessing as mp
import argparse, sys, os, time, json, subprocess, re, itertools, random, itertools, codecs, regex
from common import str2bool, timewatch, multi_process, flatten, dump_as_json, setup_parser, stanford_tokenizer

try:
   import pickle as pickle
except:
   import pickle

RED = "\033[31m"
BLUE = "\033[34m"
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
SYMLINK = '@LINK'


############################################
##              Main
############################################


@timewatch
def read_db(target_dir, dbuser, dbpass, dbhost, dbname):
  '''
  <Memo>
  - PageとRedirectのnamespaceはとりあえず両方0のみでok (https://en.wikipedia.org/wiki/Wikipedia:Namespace)
  - wbc.eu_aspectについては多分 'S' だけでok (namespace=0ならほぼ常に'S'は存在し、重複もしない) (https://www.mediawiki.org/wiki/Wikibase/Schema/wbc_entity_usage)
  - (All, T, X, S, O) = (8228959, 1401357, 1619537, 3374147, 1294316)
  '''

  target_path = target_dir + '/title2qid.txt'
  if os.path.exists(target_path): 
    sys.stderr.write('Found \'title2qid.txt\' ...\n')
    title2qid = {}
    for l in open(target_path):
      k, v = l.replace('\n', '').split('\t')
      title2qid[k] = v

    sys.stderr.write("Number of titles with entity-id: %d \n" % len(title2qid))
  else:
    import MySQLdb
    sys.stderr.write('Reading wikipedia DB...\n')
    conn = MySQLdb.connect(
      user=dbuser,
      passwd=dbpass,
      host=dbhost,
      db=dbname)
    c = conn.cursor()

    '''
    Concatenate page_title (Wikipedia) with entity_id (Wikidata) except redirects. 
    <About conditions>
    - page_namespace = 0 : use only the pages belonging to the standard namespace. Others tend to be help pages, templates, administrators, etc.
    - page_is_redirect = 0 : a redirect page doesn't have a link with eu_entity_id.
    - eu_aspect = 'S'
    '''
    sql = "select page.page_title, wbc.eu_entity_id from page inner join wbc_entity_usage as wbc on page.page_id = wbc.eu_page_id where page.page_namespace=0 and page.page_is_redirect=0 and wbc.eu_aspect='S';"
    
    c.execute(sql)
    title2qid = {}
    for row in c.fetchall():
      title, qid = row
      # Byte to str
      title = title.decode('utf-8')
      qid = qid.decode('utf-8')
      title2qid[title] = qid
    sys.stderr.write("Number of titles with entity-id: %d \n" % len(title2qid))

    # Add redirects. 
    sql = "select page.page_title, rd.rd_title from page inner join redirect as rd on page.page_id = rd.rd_from where page.page_namespace=0 and rd.rd_namespace=0;"
    c.execute(sql)
    
    for row in c.fetchall():
      from_title, to_title = row
      from_title = from_title.decode('utf-8')
      to_title = to_title.decode('utf-8')
      if to_title in title2qid:
        qid = title2qid[to_title]
        title2qid[from_title] = qid
    sys.stderr.write("Number of titles with entity-id (after redirects are added): %d \n" % len(title2qid))
    c.close()
    conn.close()
    with open(target_path, 'w') as f:
      for k, v in title2qid.items():
        line = '%s\t%s\n' % (k, v)
        f.write(line)
    #pickle.dump(title2qid, open(target_path, 'wb'))
  sys.stderr.write('Finish loading \'title2qid.txt\'.')
  return title2qid

def color_link(text, link_spans):
  text = text.split()
  for qid, start, end in link_spans:
    text[start] = RED + text[start]
    text[end] = text[end] + RESET
    title = qid2title[qid] if qid in qid2title else qid
    text[end] += ' ' + BLUE + '(%s)' % title + RESET
  return ' '.join(text)

def process_sentence(sent, links_in_para):
  '''
  - sent: A string.
  - links_in_para: a list of tuple of strings, (anchored_text, target_title).
  '''
  sent = sent.split()
  link_idxs = [i for i, w in enumerate(sent) if w == SYMLINK]
  offset = 0
  links_in_sent = []
  for idx, (target_title, anchored_text) in zip(link_idxs, links_in_para):
    sent[idx] = anchored_text
    n_words_of_anchored_text = len(anchored_text.split()) - 1 # len(anchored_text.split() - len([SYMLINK]))
    if target_title in title2qid:
      links_in_sent.append((title2qid[target_title], idx+offset, idx + offset + n_words_of_anchored_text))
    offset += n_words_of_anchored_text

  sent = ' '.join(sent).replace(LRB, '(').replace(RRB, ')').replace(LSB, '[').replace(RSB, ']').replace(LCB, '{').replace(RCB, '}')

  n_links = len(link_idxs)
  links_in_para = links_in_para[n_links:] # The found links in the sentence is removed from the link list of the paragraph.
  return sent, links_in_sent, links_in_para


def to_title_format(title_str):
  res = title_str.replace(' ', '_')
  return res[0].upper() + res[1:]


#rec_parentheses = regex.compile("(?<rec>\((?:[^\(\)]+|(?&rec))*\))")
#rec_parentheses = re.compile("(\([^\(\)]+\))")
#partial_link = re.compile("(\|.+?)(\s*?\]\])([A-Za-z0-9]+)")
rec_parentheses = regex.compile('\(([^()]|(?R))*\)')
partial_link_right = re.compile("(\|.+?)(\s*?\]\])([^\.\,\-\s\(\)\{\}\[\]]+)")
partial_link_left = re.compile("(\S+?)(\[\[)")
link_template = re.compile("(\[\[(.+?)\|([^\[\]]+?)\]\])")
link_with_extra_bracket = re.compile("\|(.*?)\[\[(.+?)\]\]\]\]")
link_with_no_surface = re.compile("\[\[[^\[]+?\|\]\]")


exists_something_or_not = "[^\[]*?"
exists_something = "[^\[]+?"
link_in_title = re.compile("\|(%s)\[\[(%s)\|(%s)\]\](%s)\]\]" % (exists_something_or_not, exists_something, exists_something, exists_something_or_not))

def process_paragraph(pid, ptitle, para_idx, paragraph, s_parser):
  para = origin = paragraph 

  # NOTE: there preprocess below must be done in this order and before tokenization.
  ####################################################################
  # Remove wrong, extra brackets. (e.g. '[[Atlas (disambiguation)|[[atlas]]]]' -> '[[Atlas (disambiguation)|atlas]]')
  para = link_with_extra_bracket.sub(r'|\1 \2]]', para)

  para = link_with_no_surface.sub('', para)
  para = link_in_title.sub(r'|\1 \3 \4]]', para)

  # Enclose the unlinked strings at the right of brackets too, otherwise a piece of characters remain. (e.g. "[[...|beverage]]s" -> "[[...|beverages]]")
  para = partial_link_right.sub(r'\1\3\2 ', para)

  # Separate the strings at the left of brackets. (e.g. 'anti-[[war|war]]' -> 'anti- [war|war]')
  para = partial_link_left.sub(r'\1 \2 ', para) 

  # Remove phrases enclosed in parentheses since those are usually expressions in different languages, or acronyms. Note that the enclosed phrase, which is a part of an article's title should not be removed for precise matching. (e.g. [[before Christ (BC)|BC]])

  #para = rec_parentheses.sub('', para)
  offset = 0
  for m in rec_parentheses.finditer(para):
    link_end_idx = m.span()[1] - offset
    if link_end_idx == len(para) or para[link_end_idx] != '|':
      para = para.replace(m.group(0), '', 1)
      offset += m.span()[1] - m.span()[0]



  # Get titles and anchored text, replace them to a special token not to be changed or separated into different sentences by tokenizer.
  links_in_para = []
  for m in link_template.findall(para):
    para = para.replace(m[0], SYMLINK)
    # Apply tokenizer for anchored texts separately.
    links_in_para.append(
      (to_title_format(m[1]), ' '.join(stanford_tokenizer(m[2], s_parser)))
    )


  ###################################################################

  # Tokenize by stanford parser.
  para = stanford_tokenizer(para, s_parser)
  if args.n_sentence:
    para = para[:args.n_sentence]

  results = []
  for s in para:
    sent, links_in_sent, links_in_para  = process_sentence(s, links_in_para)
    results.append((sent, links_in_sent))

  # Show an article for debug.
  if args.debug:
    qid = title2qid[ptitle] if ptitle in title2qid else 'None'
    idx = "%s (%s:%d)" % (ptitle, qid, para_idx)
    sys.stdout.write("%s%s Original%s: %s\n" % (BOLD, idx, RESET, origin))
    for sent_idx, (sent, links_in_sent) in enumerate(results):
      idx = "%s (%s:%d-%d)" % (ptitle, qid, para_idx, sent_idx)
      sys.stdout.write("%s%s Processed%s: %s\n"  % (
        BOLD, idx, RESET, color_link(sent, links_in_sent)))
      sys.stdout.write("%s%s Links%s: %s\n" % (
        BOLD, idx, RESET, links_in_sent))
      sys.stdout.write("\n")

  return results 

def process_page(page, s_parser):
  pid = page['pid']
  text = page['text']
  title = page['title']
  paragraphs = [line for line in text.split('\n') if len(line) > 0 and line != '　']

  # Use the second paragraph (the first paragraph is only the title.)
  if len(paragraphs) <= 1:
    return None
  res = []

  paragraphs = paragraphs[1:1+args.n_paragraph] if args.n_paragraph else paragraphs[1:]
  
  for para_idx, para in enumerate(paragraphs):
    res_paragraph = process_paragraph(pid, title, para_idx, para, s_parser)

    # Include paragraphs that include no links for now (the context may be used for something).
    res.append(res_paragraph)
  return res   # res[paragraph_id][sentence_idx] = (text, link_spans)


def read_file(source_path, s_parser):
  if args.debug:
    sys.stdout.write(source_path + '\n')
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

def read_articles(pathes, s_parser):
  res = OrderedDict()
  if not args.debug:
    pbar = tqdm(range(len(pathes)))
    for path in pathes:
      res.update(read_file(path, s_parser))
      pbar.update(1)
    pbar.close()
  else:
    for path in pathes:
      res.update(read_file(path, s_parser))



  return res

def read_all_pages(corenlp_host, corenlp_port):
  sys.stderr.write('Reading articles ...\n')
  all_pathes = str(subprocess.getoutput('ls -d %s/*/wiki_*' % args.source_dir)).split()
  if args.max_wikifiles:
    all_pathes = all_pathes[:args.max_wikifiles]

  sys.stderr.write("Number of json files: %d \n" % len(all_pathes))

  res = OrderedDict({})
  count = 1
  n_finished_files = 0
  s_parsers = [setup_parser(corenlp_host, corenlp_port) for _ in range(args.n_process)]

  n_per_process = len(all_pathes) // args.n_process + 1
  pathes = [all_pathes[i:i+n_per_process] for i in range(0, len(all_pathes), n_per_process)]
  res_process = multi_process(read_articles, pathes, s_parsers)

  for r in res_process:
    res.update(r)
  for parser in s_parsers:
    parser.close()
  return res

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
def main(args):
  output_dir = args.output_dir
  output_file = 'articles'
  output_path = os.path.join(output_dir, output_file)
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)
  # Create json dumps.
  if not os.path.exists(output_path):
    global title2qid
    global qid2title
    title2qid = read_db(args.source_dir, args.dbuser, args.dbpass, 
                        args.dbhost, args.dbname)
    qid2title = {v:k for k, v in title2qid.items()}
    pages = read_all_pages(args.corenlp_host, args.corenlp_port)
  else:
    sys.stderr.write('Found dump files. \n') 
    return

  # create a json dump.
  dump_as_json(pages, output_path + '.json', False)
  dump_as_json(pages, output_path + '.jsonlines', True)

if __name__ == "__main__":
  desc = "This script creates wikiP2D corpus from Wikipedia dump files. They contain sql files (page.sql, wbc_entity_usage.sql, redirect.sql), which must be stored to MySQL in advance, and pages-articles.xml parsed by WikiExtractor.py (https://github.com/attardi/wikiextractor.git) with '--filter_disambig_pages --json' options."
  parser = argparse.ArgumentParser(description=desc)

  # MySQL
  parser.add_argument('dbuser',
                      help='the username of your MySQL account')
  parser.add_argument('dbpass',
                      help='the password of your MySQL account')
  parser.add_argument('--dbhost', default='localhost',
                      help='the host where MySQL works.')
  
  parser.add_argument('--dbname', default='wikipedia',
                      help='the name of database where you stored the dump sqls.')

  # Input and Output dirs
  parser.add_argument('-s', '--source_dir', default='wikipedia/latest/extracted', 
                      help='the root directory whose subdirectories (AA, AB, ...) contain wiki_** files parsed by WikiExtractor.py from enwiki-***-pages-articles.xml')
  parser.add_argument('-o', '--output_dir', default='wikipedia/latest/extracted/dump')

  # The maximum number of files, paragraphs, and sentences
  parser.add_argument('-mw', '--max_wikifiles', default=0, type=int)
  parser.add_argument('-npg','--n_paragraph', default=0, type=int, help='if None, this script reads all paragraphs in the paragraph.')
  parser.add_argument('-nst','--n_sentence', default=0, type=int, help='if None, this script reads all sentences in the paragraph.')

  # MultiProcessing
  parser.add_argument('-npr','--n_process', default=16, type=int)

  # CoreNLP
  parser.add_argument('-ch', '--corenlp_host', default='http://localhost', type=str)
  parser.add_argument('-cp', '--corenlp_port', default=9000, type=int)

  parser.add_argument('--debug', default=True, type=str2bool)
  #parser.add_argument('--cleanup', default=False, type=str2bool)

  args = parser.parse_args()
  main(args)

