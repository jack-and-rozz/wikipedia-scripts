# coding: utf-8
from collections import OrderedDict, defaultdict
import argparse, sys, json

def read_json_lines(path):
    res = OrderedDict()
    with open(path) as f:
        for line in f:
            j = json.loads(line)
            res[j['qid']] = j
    return res

def read_triples(path):
    data = defaultdict(list)
    for line in open(path):
        subj, rel, obj = line.strip().split('\t')
        data[(subj, obj)].append(rel)
    return data

def dump_as_json(entities, file_path, as_jsonlines=True):
    if as_jsonlines:
        with open(file_path, 'a') as f:
            for entity in entities.values():
                json.dump(entity, f, ensure_ascii=False)
                f.write('\n')
    else:
        with open(file_path, 'w') as f:
            json.dump(entities, f, indent=4, ensure_ascii=False)

def merge_data(pages, items, triples):
    merged = {}
    for qid_subj in pages:
        # copy items in pages.all.jsonlines
        merged[qid_subj] = pages[qid_subj]

        # extract triples from triples.txt and add them to merged data
        merged[qid_subj]['triples'] = []
        for qid_obj in pages[qid_subj]['link']:
            for key in [(qid_subj, qid_obj), (qid_obj, qid_subj)]: # find both of (subj, rel, obj) and (obj, rel, subj)
                if len(triples[key]) > 0:
                    for rel in triples[key]:
                        merged[qid_subj]['triples'].append((key[0], rel, key[1]))

        # extract definitions from items.tokenized.jsonlines and add them to merged data
        merged[qid_subj]['desc'] = ''
        if qid_subj in items:
            merged[qid_subj]['desc'] = items[qid_subj]['desc']

    return merged

def main(args):
    sys.stdout.write("Reading data...\n")
    pages = read_json_lines(args.wp_source_dir + '/pages.all.jsonlines')
    items = read_json_lines(args.wd_source_dir + '/items.tokenized.jsonlines')
    triples = read_triples(args.wd_source_dir + '/triples.txt')
    sys.stdout.write("Number of Pages, Items, Triples = %d, %d, %d\n" % (len(pages), len(items), len(triples)))

    sys.stdout.write("Merging data...\n")
    merged = merge_data(pages, items, triples)
    sys.stdout.write("Dumping data...\n")
    dump_as_json(merged, args.target_dir + '/merged.jsonlines', as_jsonlines=True)
    dump_as_json(merged, args.target_dir + '/merged.json', as_jsonlines=False)
    sys.stdout.write("Done. Output files: %s\n (and *.jsonlines)\n" % args.target_dir + '/merged.json')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script that merges pages.all.jsonlines, items.tokenized.jsonlines, and triples.txt together into merged.{json|jsonlines}.')
    parser.add_argument('--wp_source_dir', default='wp.dumps.p1s0', help='directory that includes outputs of wp_extract_all.py')
    parser.add_argument('--wd_source_dir', default='wd.dumps.all', help='directory that includes  outputs of wd_extract_all.py')
    parser.add_argument('target_dir', help='path of output files')
    args = parser.parse_args()
    main(args)
