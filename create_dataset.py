# coding:utf-8
from collections import defaultdict, Counter
from pprint import pprint
import common
from common import RED, RESET, BLUE, BOLD, UNDERLINE, GREEN
import argparse, collections, re, os, time, sys, codecs, json, random, copy
random.seed(0)

@common.timewatch
def read_jsonlines(source_path, max_rows=0):
  data = {}
  for i, l in enumerate(open(source_path)):
    if max_rows and i >= max_rows:
      break
    d = json.loads(l)
    data.update({d['qid']:d})
  return common.recDotDict(data)

def read_json(source_path):
  data = json.load(open(source_path)) 
  return common.recDotDict(data)


def statistics(data):
  link_cnt = defaultdict(int)
  triple_cnt = defaultdict(int)
  entity_cnt = defaultdict(int)
  relation_cnt = defaultdict(int)
  sent_cnt = defaultdict(int)
  for a_qid, d in data.items():
    for l_qid in d.link:
      link_cnt[l_qid] += 1
    for s, r, o in d.triples:
      triple_cnt[(s,r,o)] += 1
      entity_cnt[s] += 1
      entity_cnt[o] += 1
      relation_cnt[r] += 1
    n_sentences = len(d.text)
    sent_cnt[n_sentences] += 1
  def _sort(counter):
    return sorted(counter.items(), key=lambda x:-x[1])

  sorted_stat = [_sort(x) for x in [sent_cnt, link_cnt, triple_cnt, entity_cnt, relation_cnt]]
  sent_cnt, link_cnt, triple_cnt, entity_cnt, relation_cnt = sorted_stat 

  sys.stdout = sys.stderr
  print('# of articles: ', len(data))
  print('# of triples: ', sum([v for k, v in triple_cnt]))
  print('# of links: ', sum([v for k, v in link_cnt]))
  print('# of sentences: ', sum([k*v for k, v in sent_cnt]))
  print() 
  sys.stdout = sys.__stdout__
  return sorted_stat 


@common.timewatch
def remove_useless_articles(data, args):
  def condition(d):
    if len(d.triples) == 0:
      return False
    if args.min_n_links and len(d.link) < args.min_n_links:
      return False
    if args.max_n_sentences and len(d.text) > args.max_n_sentences:
      return False
    if args.max_n_words and [sent for sent in d.text if len(sent.split()) > args.max_n_words]:
      return False
    if args.min_n_words and [sent for sent in d.text if len(sent.split()) < args.min_n_words]:
      return False

    return True
  new_data = {k:v for k, v in data.items() if condition(v)}
  return common.recDotDict(new_data)


@common.timewatch
def remove_useless_triples(data, major_relations):
  '''
  Remove triples which contain a minor (not in top-N frequent ones) relation, two or more links as triple-related entities of a certain title-relation pair.
  e.g. in article 'Q5692653',
       ['Q5692653', 'P175', 'Q6766836'],
       ['Q5692653', 'P175', 'Q1166670']
  '''
  def select(d):
    # Remove minor ones.
    triples = set([(s,r,o) for (s,r,o) in d.triples if r in major_relations])
    relations_in_an_article = set([r for s,r,o in triples])

    # Remove duplicating ones.
    for r in relations_in_an_article:
      # Check (titled_entity, rel, ?)
      checked_triples = [tpl for tpl in triples if tpl[0] == d.qid and tpl[1] == r]
      if len(checked_triples) > 1:
        triples -= set(checked_triples)
      # Check (?, rel, titled_entity)
      checked_triples = [tpl for tpl in triples if tpl[2] == d.qid and tpl[1] == r]
      if len(checked_triples) > 1:
        triples -= set(checked_triples)

    d.triples = list(triples)
    return d
  new_data = {k:select(v) for k, v in data.items()}
  return common.recDotDict(new_data)


def dump_as_json(entities, file_path, as_jsonlines=True):
  if as_jsonlines:
    if os.path.exists(file_path):
      os.system('rm %s' % file_path)
    with open(file_path, 'a') as f:
      for entity in entities.values():
        json.dump(entity, f, ensure_ascii=False)
        f.write('\n')
  else:
    with open(file_path, 'w') as f:
      json.dump(entities, f, indent=4, ensure_ascii=False)

def divide_data(data, n_train, n_dev, n_test):
  assert n_train + n_dev + n_test <= len(data)
  article_keys = set(data.keys())
  train_keys = random.sample(article_keys, n_train)
  article_keys -= set(train_keys)
  dev_keys = random.sample(article_keys, n_dev)
  article_keys -= set(dev_keys)
  test_keys = random.sample(article_keys, n_test)

  train = {k:data[k] for k in train_keys}
  dev = {k:data[k] for k in dev_keys}
  test = {k:data[k] for k in test_keys}
  return train, dev, test

def preprocess(data):
  
  def _preprocess_title(title): 
    # TODO: This operation should be done in wp.extract_all.py, but for now we do here for convenience.
    title = ' '.join([w for w in title.split() if w])
    title = title.replace('_', ' ').replace("'", " '").replace(':', ' :').replace(',', ' ,').replace('(', ' ( ').replace(')', ' ) ')
    title = ' '.join([w for w in title.split() if w])
    return title

  for k in data:
    data[k].title = _preprocess_title(data[k].title)
    data[k].text = data[k].text[0]
    data[k].link = {l_qid:(l_sid, (l_begin, l_end)) for l_qid, (l_pid, l_sid, (l_begin, l_end)) in data[k].link.items()}
  return data

def select_triple(data, allow_triple_duplication):
  new_data = {}
  data = [(k, v) for k, v in data.items()]
  random.shuffle(data)
  selected_triples = set() # Choose only from unselected triples.
  for _, d in data:
    triple_candidates = [t for t in d.triples if t[0] == d.qid]
    if not allow_triple_duplication:
      triple_candidates = [t for t in triple_candidates if t not in selected_triples]
    if len(triple_candidates) > 0:
      d.positive_triple = random.choice(triple_candidates)
      pos_subj, pos_rel, pos_obj = d.positive_triple
      if pos_subj == d.qid: # (title, rel, ?) 
        neg_obj = random.choice([l_qid for l_qid in d.link if l_qid not in [d.qid, pos_obj]])
        d.negative_triple = (pos_subj, pos_rel, neg_obj)
      else:  # (?, rel, title)
        neg_subj = random.choice([l_qid for l_qid in d.link if l_qid not in [d.qid, pos_subj]])
        d.negative_triple = (neg_obj, pos_rel, pos_obj)

      assert d.positive_triple and d.negative_triple
      selected_triples.add(d.positive_triple)
      new_data[d.qid] = d
  try:
    assert len([d for d in new_data.values() if not d.positive_triple]) == 0
  except:
    print(len([d for d in new_data.values() if not d.positive_triple]))
    exit(1)
  return common.recDotDict(new_data)
  

def color_link(data):
  def _color_link(text, links):
    '''
    text : A list of sentence. a sentence is a list of words, or a string.
    links: A list of (sentence_id, begining of the link, end of the link).
    '''
    if isinstance(text[0], str):
      text = [s.split() for s in text]
    for sent_id, (start, end) in links:
      text[sent_id][start] = RED + text[sent_id][start]
      text[sent_id][end] = text[sent_id][end] + RESET
    return [' '.join(s)for s in text]

  new_data = {}
  for qid, d in data.items():
    new_data[qid] = d
    new_data[qid].text = _color_link(d.text, d.link.values())
  return common.recDotDict(new_data)

def print_colored(data, props):
  colored_data = color_link(data)
  for i, (k,d) in enumerate(colored_data.items()):
    print("<title>  \t", d.title)

    str_triples = []
    for s, r, o in d.triples:
      title = BOLD + d.title + RESET
      if s == d.qid:
        subj = title
        sid, (begin, end) =  d.link[o]
        obj = '_'.join(d.text[sid][begin:end+1])
      else:
        obj = title
        sid, (begin, end) =  d.link[s]
        subj = '_'.join(d.text[sid][begin:end+1])
      str_triples.append((subj, props[r].name, obj))
    print("<triples>\t", ' '.join(['(%s, %s, %s)' % (s,r,o) for s,r,o in str_triples]))
    print("<text>   \t", '\n'.join(d.text))

    # apply mask.
    sid, (begin, end) = d.link[d.positive_triple[2]]
    d.text[sid] = ' '.join([BLUE + '_UNK' + RESET if j in range(begin, end+1)  else w for j, w in enumerate(d.text[sid].split())])

    sid, (begin, end) = d.link[d.negative_triple[2]]
    d.text[sid] = ' '.join([GREEN + '_UNK' + RESET if j in range(begin, end+1)  else w for j, w in enumerate(d.text[sid].split())])
    print("<masked text>\t", '\n'.join(d.text))
  
@common.timewatch
def match_title_with_text(data):
  def _match_span(span, title):
    def _create_set(word):
      return set([word, word+'s', word+'es'])
    # for plural
    for w1, w2 in zip(span, title):
      w1set = _create_set(w1)
      w2set = _create_set(w2)
      if not w1set.intersection(w2set):
        return False
    return True

  def _match_title_with_text(text, title):
    for i in range(len(text) - len(title) + 1):
      span = text[i:i+len(title)]
      if _match_span(span, title):
        begin = i
        end = begin + len(title) -1 
        assert begin >= 0 and end >= 0
        return (0, (begin, end))

  # Create suedo-self-link by rule-based matching.
  new_data ={}
  unmatched = {}
  for a_qid, d in data.items():
    text = d.text[0].lower()
    title = d.title.lower()

    matched_position = _match_title_with_text(text.split(), title.split())
    if not matched_position:
      # If not matched, another preproessings are applied to title (remove parenthees, etc.)
      if '(' in title:
        title = re.sub('\s?\( .+ \)\s?', ' ', title)
        title = ' '.join([x for x in title.split()])
        if title:
          matched_position = _match_title_with_text(text.split(), title.split())

    if not matched_position:
      if ',' in title:
        title = title.split(',')[0].strip()
        matched_position = _match_title_with_text(text.split(), title.split())

    if matched_position:
      d.link[a_qid] = matched_position
      new_data[a_qid] = d
    else:
      unmatched[a_qid] = d

  return common.recDotDict(new_data), common.recDotDict(unmatched)

@common.timewatch
def main(args):
  props = read_jsonlines(os.path.join(args.source_dir, 'properties.tokenized.jsonlines'))
  data = read_jsonlines(os.path.join(args.source_dir, args.filename), 
                        max_rows=args.max_rows)
  # Use only the first paragraph.
  data = preprocess(data)

  sent_cnt, link_cnt, triple_cnt, entity_cnt, relation_cnt = statistics(data)
  sys.stderr.write('All articles.\n')

  if args.n_relations:
    major_relations = set([k for k, v in relation_cnt[:args.n_relations]])
    data = remove_useless_triples(data, major_relations, )
    sys.stderr.write('After removing minor triples.\n')
    statistics(data)

  data = remove_useless_articles(data, args)
  words_cnt = defaultdict(int)
  for k,d in data.items():
    for s in d.text:
      words_cnt[len(s.split())] += 1

  sys.stderr.write('After removing useless articles.\n')
  sent_cnt, link_cnt, triple_cnt, entity_cnt, relation_cnt = statistics(data)
  words_cnt = sorted(list(words_cnt.items()), key=lambda x:x[0])

  data, unmatched = match_title_with_text(data)
  sys.stderr.write('\nAfter removing title-unmatched articles.\n')
  sys.stderr.write('# of articles (matched, unmatched):\t%d %d\n' % (len(data), len(unmatched)))

  data = select_triple(data, args.allow_triple_duplication) # For now, use only one randomly selected triple.
  sys.stderr.write('\nAfter removing articles which have duplicate question-triple.\n')
  statistics(data)

  if args.debug:
    print_colored(data, props)
    exit(1)

  train, dev, test = divide_data(data, args.n_train, args.n_dev, args.n_test)
  sys.stderr.write('\nTraining data.\n')
  statistics(train)
  sys.stderr.write('\nDevelopment data.\n')
  statistics(dev)
  sys.stderr.write('\nTesting data.\n')
  statistics(test)

  target_dir = args.target_dir if args.target_dir else args.source_dir
  if not os.path.exists(target_dir):
    os.makedirs(target_dir)

  dump_as_json(train, os.path.join(target_dir, 'train.jsonlines'))
  dump_as_json(dev, os.path.join(target_dir, 'dev.jsonlines'))
  dump_as_json(test, os.path.join(target_dir, 'test.jsonlines'))

  dump_as_json(train, os.path.join(target_dir, 'train.json'), as_jsonlines=False)
  dump_as_json(dev, os.path.join(target_dir, 'dev.json'), as_jsonlines=False)
  dump_as_json(test, os.path.join(target_dir, 'test.json'), as_jsonlines=False)

if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--n_train', type=int, default=500000)
  parser.add_argument('--n_dev', type=int, default=1000)
  parser.add_argument('--n_test', type=int, default=1000)

  parser.add_argument('-s', '--source_dir',
                      default='wikiP2D/wikiP2D.p1s0',
                      help='')
  parser.add_argument('-t', '--target_dir', default='')
  parser.add_argument('-f', '--filename', default='merged.jsonlines')
  parser.add_argument('-mr', '--max_rows', type=int, default=0)
  parser.add_argument('-ml', '--min_n_links', type=int, default=3)
  parser.add_argument('-nr', '--n_relations', type=int, default=400)

  parser.add_argument('--min_n_sentences', type=int, default=0)
  parser.add_argument('--max_n_sentences', type=int, default=5)
  parser.add_argument('--min_n_words', type=int, default=0)
  parser.add_argument('--max_n_words', type=int, default=50)

  parser.add_argument('--allow_triple_duplication', type=common.str2bool, 
                      default=False)
  parser.add_argument('--debug', type=common.str2bool, default=False)
  args = parser.parse_args()
  main(args)
