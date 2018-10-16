# coding: utf-8
from collections import OrderedDict, defaultdict
import argparse, sys, json, os
from common import dump_as_json, recDotDict, dbgprint, timewatch, str2bool

@timewatch
def read_jsonlines(path, max_rows=None):
    res = OrderedDict()
    for i, line in enumerate(open(path)):
        if max_rows and i >= max_rows:
            break
        j = recDotDict(json.loads(line))
        res[j['qid']] = j
    return res

@timewatch
def read_triples(path, max_rows=None):
    data = defaultdict(list)
    for i, line in enumerate(open(path)):
        if max_rows and i >= max_rows:
            break
        subj, rel, obj = line.strip().split('\t')
        data[(subj, obj)].append(rel)
    return data

def merge_wikidata(pages, items, triples, contexts):
    merged = OrderedDict()
    for qid in pages:
        # copy items in pages.all.jsonlines
        
        merged[qid] = pages[qid]
        if qid in items:
            merged[qid]['name'] = items[qid]['name']
            if args.merge_aka and qid:
                merged[qid]['aka'] = items[qid]['aka']

        # extract triples from triples.txt and merge them.
        if args.merge_triples:
            merged[qid]['triples'] = []
            for target_qid in pages[qid]['link']:
                for key in [(qid, target_qid), (target_qid, qid)]: # find both of (subj, rel, obj) and (obj, rel, subj)
                    if len(triples[key]) > 0:
                        for rel in triples[key]:
                            merged[qid]['triples'].append((key[0], rel, key[1]))

        # extract definitions from items.tokenized.jsonlines and add them to merged data
        if args.merge_desc:
            merged[qid]['desc'] = ''
            if qid in items:
                merged[qid]['desc'] = items[qid]['desc']
        if args.merge_contexts:
            merged[qid]['contexts'] = contexts[qid]
        d = merged[qid]
        n_sents = sum([len(p) for p in d.text])
        n_contexts = len(d.contexts)
        #print(qid, n_sents, n_contexts)
    return merged


@timewatch
def extract_contexts(data):
  contexts_by_qid = defaultdict(list)
  for qid, v in data.items():
    for target_qid, position in v.link.items():
      para, sent, span = position
      if target_qid != qid:
          contexts_by_qid[target_qid].append((v.text[para][sent], span))
  return contexts_by_qid



def main(args):
    sys.stdout.write("Reading data...\n")
    pages = read_jsonlines(args.wp_source_dir + '/articles.jsonlines', max_rows=args.max_rows)
    items = read_jsonlines(args.wd_source_dir + '/items.jsonlines', max_rows=args.max_rows)
    triples = read_triples(args.wd_source_dir + '/triples.txt', max_rows=args.max_rows) if args.merge_triples else []

    sys.stdout.write("Number of Pages, Items, Triples = %d, %d, %d\n" % (len(pages), len(items), len(triples)))

    contexts = extract_contexts(pages) # A dict keyed by Qid, whose values are a list of sentences with an anchored text to the item.

    # k1 = set(sorted(pages.keys()))
    # k2 = set(sorted(items.keys()))
    # k3 = set(sorted(contexts.keys()))
    # print(len(k1), len(k2), len(k3))
    # print(len(k1.intersection(k2)), len(k1.intersection(k3)))

    sys.stdout.write("Merging data...\n")
    merged = merge_wikidata(pages, items, triples, contexts)
    statistics(merged)
    sys.stdout.write("Dumping data...\n")
    if not os.path.exists(args.target_dir):
        os.makedirs(args.target_dir)
    dump_as_json(merged, args.target_dir + '/merged.jsonlines', as_jsonlines=True)
    dump_as_json(merged, args.target_dir + '/merged.json', as_jsonlines=False)
    sys.stdout.write("Done. Output files: %s (and *.jsonlines)\n" % args.target_dir + '/merged.json')

def statistics(data):
    n_entities = len(data)
    n_descs = len([d.desc for d in data.values() if d.desc.strip()])
    n_sents = sum([sum([len(p) for p in d.text]) for d in data.values()])
    n_contexts = sum([len(d.contexts) for d in data.values()])
    print('# of entities:\t', n_entities)
    print('# of entities with description:\t', n_descs)
    print('# of entities:\t', n_entities)
    print('# of sentences in articles:\t%d (Avg = %f)' % (n_sents, n_sents/n_entities))
    print('# of contexts:\t%d (Avg = %f)' % (n_contexts, n_contexts/n_entities))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script that merges pages.all.jsonlines, items.tokenized.jsonlines, and triples.txt together into merged.{json|jsonlines}.')
    parser.add_argument('-wps', '--wp_source_dir', 
                        default='wikipedia/latest/extracted/dumps.p0s0', 
                        help='The directory including outputs wp parsed data.')
    parser.add_argument('-wds', '--wd_source_dir', 
                        default='wikidata/latest/extracted', 
                        help='The directory including outputs wd parsed data.')
    parser.add_argument('-t', '--target_dir', 
                        default='wikiP2D/latest',
                        help='path of output files')
    parser.add_argument('-mr', '--max_rows', default=None, type=int, 
                        help='The maximum lines to be read from the sources. Mainly for debugging.')
    parser.add_argument('--merge_triples', default=False, type=str2bool)
    parser.add_argument('--merge_desc', default=True, type=str2bool)
    parser.add_argument('--merge_aka', default=True, type=str2bool)
    parser.add_argument('--merge_contexts', default=True, type=str2bool)
    args = parser.parse_args()
    main(args)
