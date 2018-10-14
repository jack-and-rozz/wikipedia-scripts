# coding: utf-8
from pprint import pprint
from collections import OrderedDict, defaultdict
from getpass import getpass
import multiprocessing as mp
import argparse, sys, os, time, json, subprocess, re, itertools, random
from common import str2bool, timewatch, multi_process

try:
   import pickle as pickle
except:
   import pickle


@timewatch
def read_triples(triples_path, max_rows):
  triples = defaultdict(str)
  forwards = defaultdict(list)
  backwards = defaultdict(list)
  for i, l in enumerate(open(triples_path)):
    if max_rows and i >= max_rows:
      break
    s, r, o = l.replace('\n', '').split('\t')
    triples[(s, o)] = r
    forwards[s].append((r, o))
    backwards[o].append((s, r))
  return triples, forwards, backwards

@timewatch
def read_articles(articles_path, max_rows):
  articles = OrderedDict()
  for i, l in enumerate(open(articles_path)):
    if max_rows and i >= max_rows:
      break
    article = json.loads(l)
    qid = article['qid']
    articles[qid] = article
  return articles

@timewatch
def count_triples_in_article(articles, triples, forwards, backwards):
  num_all_triples = 0
  num_mentions_related_triples = 0
  num_mentions_unrelated_triples = 0


  # Whether a triple is mentioned in an article.
  mentioned_in_article = defaultdict(bool)
  mentioned_out_of_article = defaultdict(bool)
  for s, o in triples.keys():
    mentioned_in_article[(s, o)] = False
    mentioned_out_of_article[(s, o)] = False
  
  for a_qid, article in articles.items():
    related_triples = [(a_qid, r, o) for r, o in forwards[a_qid]] + [(s, r, a_qid) for s, r in backwards[a_qid]] # triples related to a_qid and registered in wikidata
    in_article_related_triples = [] # triples related to a_qid and l_qid (the entity appearing the article).
    for l_qid in article['link']:
      rel = triples[(a_qid, l_qid)]
      if rel:
        in_article_related_triples.append((a_qid, rel, l_qid))

      rel = triples[(l_qid, a_qid)]
      if rel:
        in_article_related_triples.append((l_qid, rel, a_qid))

      for (s, r, o) in in_article_related_triples:
        mentioned_in_article[(s, o)] = True

    in_article_unrelated_triples = []
    for (s, o) in list(itertools.combinations(article['link'].keys(), 2)):
      rel = triples[(s, o)]
      if rel:
        in_article_unrelated_triples.append((s, rel, o))
        mentioned_out_of_article[(s, o)] = True

    num_mentions_unrelated_triples += len(in_article_unrelated_triples)

    num_all_triples += len(related_triples) # number of all the triples should be that of the entities which have an article about themselves.

    num_mentions_related_triples += len(in_article_related_triples)
    if len(related_triples) and (len(in_article_related_triples) or len(in_article_unrelated_triples)):
      related_triples = ' '.join(str(t) for t in related_triples)
      in_article_related_triples = ' '.join(str(t) for t in in_article_related_triples)
      in_article_unrelated_triples = ' '.join(str(t) for t in in_article_unrelated_triples)
      print ('All Triples                (%s):\t' % (a_qid), related_triples)
      print ('Related triples in article (%s):\t' % (a_qid), in_article_related_triples)
      print ('Other triples in article   (%s):\t' % (a_qid), in_article_unrelated_triples)

  print ('')
  print('# all triples       : %d' % num_all_triples )
  print('# mentions about the triples of main entities: %d' % num_mentions_related_triples)
  print('# mentions about the triples of sub entities: %d' % num_mentions_unrelated_triples )
  if num_all_triples:
    coverage = 100.0 * len([1 for v in mentioned_in_article.values() if v == True]) / num_all_triples
    print('Coverage of article-related triples  : %.2f%%' % (coverage))
    coverage = 100.0 * len([1 for v in mentioned_out_of_article.values() if v == True]) / num_all_triples
    print('Coverage of article-unrelated triples: %.2f%%' % (coverage))
  return


@timewatch
def main(args):
  #
  triples, forwards, backwards = read_triples(args.wd_source_path, args.wd_max_rows)
  #
  articles = read_articles(args.wp_source_path, args.wp_max_rows)
  count_triples_in_article(articles, triples, forwards, backwards)

if __name__ == "__main__":
  desc = ''
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('-wp', '--wp_source_path', default='wikipedia/latest/extracted/dumps.p1s0/pages.all.jsonlines')
  parser.add_argument('-wd', '--wd_source_path', default='wikidata/latest/extracted/all.bak/triples.txt')
  parser.add_argument('-wpm', '--wp_max_rows', default=None, type=int)
  parser.add_argument('-wdm', '--wd_max_rows', default=None, type=int)
  args = parser.parse_args()
  main(args)

# All triples: 97511494
# All articles: 2303363
