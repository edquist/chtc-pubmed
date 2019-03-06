#!/usr/bin/env python
# encoding: utf-8

import sys
import json
import datetime
import requests
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
    total = res['hits']['total']
    got = len(res['hits']['hits'])
    rem = total - got
    print time.time()
    print "got %s of %s; %s remaining" % (got, total, rem)
    scroll_id = res['_scroll_id']
    scroll_q = dict(scroll="10m", scroll_id=scroll_id)
    while res['hits']['hits']:
        yield res['hits']['hits']
        res = requests.post(scrollurl, json=scroll_q).json()
        print time.time()
        got = len(res['hits']['hits'])
        rem -= got
        print "got %s of %s; %s remaining" % (got, total, rem)

def scrollids(query, size=100):
    for hits in scrollhits(query, size):
        for hit in hits:
            yield hit['_id']

def get_count(terms, through_year, url_base):
    q = make_query_object(target_term, key_phrase, through_year)
    res = requests.get(url_base, data=json.dumps(q))
    ret_cnt = res.json()['count']
    return ret_cnt

def get_terms_synonyms(path):
    terms_synonyms = []
    with open(path) as infile:
        for line in infile:
            key, vals = line.strip().split('\t')
            if key == "Concept_ID":
                continue
            terms = vals.strip().split('|')
            #id_ = re.search(r'^[A-Z]+0*(\d+)_', key).group(1)
            terms_synonyms.append((key, terms))
    return terms_synonyms

def process_file(path):
    terms_synonyms = get_terms_synonyms(path)

    for key, terms in terms_synonyms:
        key_id = key.split('_')[0]
        print "%s\t%s" % (key_id, terms)

        q = make_query_object2(terms)
        for article_id in scrollids(q):
            print key_id, article_id
        print


def main():
    process_file(sys.argv[1])


if __name__ == '__main__':
    main()
