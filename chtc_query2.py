#!/usr/bin/env python
# encoding: utf-8

import sys
import json
import datetime
#import requests
import argparse
from random import shuffle
import os.path
from multiprocessing.dummy import Pool as ThreadPool
import time
import re


class Timer(object):
    def __init__(self, name=None):
        self.name = name

    def __enter__(self):
        self.tstart = time.time()

    def __exit__(self, type, value, traceback):
        if self.name:
            print '[%s]' % self.name,
        print 'Elapsed: %s' % (time.time() - self.tstart)

# some constants for search
#URL_BASE = 'http://localhost:9200/articles_dummy/article/_count'
#URL_BASE_search = 'http://localhost:9200/articles_dummy/article/_search'
URL_BASE = 'http://localhost:9200/articles_dummy/_count'
URL_BASE_search = 'http://localhost:9200/articles_dummy/_search'
URL_BASE_scroll = 'http://localhost:9200/_search/scroll'

baseurl   = 'http://localhost:9200'
indexurl  = baseurl  + '/articles_dummy'
counturl  = indexurl + '/_count'
searchurl = indexurl + '/_search'
scrollurl = baseurl  + '/_search/scroll'

#   if through_year:
#       must.append({'range': {'publication_date.year': {'lte': through_year}}})

def make_query_object2(terms, extra_conditions=None):
    should = []
    must = []
    for term in terms:
        should.append({'match_phrase': {'title':    term}})
        should.append({'match_phrase': {'abstract': term}})
    must.append({'bool': {'should': should}})
    if extra_conditions:
        must += extra_conditions
    query = {'query': {'bool': {'must': must }}}
    return query

def scrollhits(query, size=100):
    url = "%s?scroll=10m" % searchurl
    q = dict(query, size=size)
    print time.time()
    res = requests.post(url, json=q).json()
    scroll_id = res['_scroll_id']
    scroll_q = dict(scroll="10m", scroll_id=scroll_id)
    while res['hits']['hits']:
        yield res['hits']['hits']
        print time.time()
        res = requests.post(scrollurl, json=scroll_q).json()
        print time.time()

def scrollids(query, size=100):
    for hits in scrollhits(query, size):
        for hit in hits:
            yield hit['_id']

def get_count(terms, through_year, url_base):
    q = make_query_object(target_term, key_phrase, through_year)
    res = requests.get(url_base, data=json.dumps(q))
    ret_cnt = res.json()['count']
    return ret_cnt

def build_arg_parser():
    parser = argparse.ArgumentParser(description='Run a KinderMiner search on CHTC')
    parser.add_argument('-s', '--sep', action='store_true', help='perform keyphrase match as separate tokens')
    parser.add_argument('-y', '--year', type=int, default=datetime.datetime.now().year, help='limit search to publications through particular year')
    parser.add_argument('term_file', help='file containing all target terms and their synonyms to rank, one per line')
    parser.add_argument('keyphrase', help='key phrase and its synonyms to rank target terms against')
    parser.add_argument('output', help='directory where output file(s) are dumped')
    return parser

def get_terms_synonyms(path):
    terms_synonyms = []
    with open(path) as infile:
        infile.readline()  # skip header line
        for line in infile:
            key, vals = line.strip().split('\t')
            terms = vals.strip().split('|')
            #id_ = re.search(r'^[A-Z]+0*(\d+)_', key).group(1)
            terms_synonyms.append((key, terms))
    return terms_synonyms

def process_file(path):
    terms_synonyms = get_terms_synonyms(path)

    for key, terms in terms_synonyms:
        id_ = key.split('_')[0]
        print "%s\t%s" % (id_, terms)
        continue

        count = get_count(terms, None, URL_BASE)

        targ_cnt = get_count(target, None, THROUGH_YEAR, URL_BASE)

        # only need to query combined if articles exist for both individually
        if targ_cnt > 0 and kp_cnt > 0:
            # now do both key phrase and target
            targ_with_kp_cnt = get_count(target, key_phrase, THROUGH_YEAR, URL_BASE)

        key = target[0]
        value = target[1]
        outstr = '{0}\t{1}\t{2}\t{3}\t{4}'.format(key+':'+value[0], targ_with_kp_cnt, targ_cnt, kp_cnt, db_article_cnt)
        #print (outstr)
        out_fh.write(outstr + '\n')



def main():
    process_file(sys.argv[1])
    sys.exit(0)

    global TARGET_TERM_FILE
    global KEY_PHRASE_FILE_NAME
    global OUTPUT_DIRECTORY
    global THROUGH_YEAR
    global SEPARATE_KP
    global target_terms
    parser = build_arg_parser()
    args = parser.parse_args()
    # command line args
    TARGET_TERM_FILE = args.term_file
    KEY_PHRASE_FILE_NAME = args.keyphrase
    THROUGH_YEAR = args.year
    SEPARATE_KP = args.sep
    target_terms = get_terms_synonyms(TARGET_TERM_FILE)


    # read keyphrases from file
    key_phrases = get_terms_synonyms(KEY_PHRASE_FILE_NAME)
    #for k,v in key_phrases.items():
        #for el in v:
            #print el
    # split the keyphrase if we are searching for separate tokens
    if SEPARATE_KP:
        assert False, "have to deal with filename"
        key_phrases_asTokens = get_terms_asTokens(KEY_PHRASE_FILE_NAME)

    # read the all the target terms to query

    # compute the total number of articles in the database
    global db_article_cnt
    db_article_cnt = get_count(None, None, THROUGH_YEAR, URL_BASE)
    for n_threads in [1]:
        OUTPUT_DIRECTORY = args.output + "_%s" % n_threads
#        shuffle(key_phrases)
        print("--- %s threads ---" % n_threads)
        pool = ThreadPool(n_threads)
        with Timer("%s threads" % n_threads):
            # and the number of times the key phrase shows up
            pool.map(do_junk, key_phrases.items())

if __name__ == '__main__':
    main()
