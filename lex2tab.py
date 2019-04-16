#!/usr/bin/env python
# encoding: utf-8

import sys
import json
import time
import os
import re


baseurl   = 'http://localhost:9200'
indexurl  = baseurl  + '/articles_dummy'
counturl  = indexurl + '/_count'
searchurl = indexurl + '/_search'
scrollurl = baseurl  + '/_search/scroll'

#   if through_year:
#       must.append({'range': {'publication_date.year': {'lte': through_year}}})

def make_terms_query(terms, extra_conditions=None):
    should = []
    must = []
    for term in terms:
        should.append({'match_phrase': {'title':    term}})
        should.append({'match_phrase': {'abstract': term}})
    must.append({'bool': {'should': should}})
    if extra_conditions:
        must += extra_conditions
    query = {'query': {'bool': {'must': must }}, '_source': False}
    return query


def must_term_sep(term):
    must = []
    for x in term.split():
        if not re.search(r'[a-zA-Z0-9]', x):
            continue
        should = [{'match_phrase': {'title':    x}},
                  {'match_phrase': {'abstract': x}}]
        must.append({'bool': {'should': should}})
    return {'bool': {'must': must}}


def make_terms_query_sep(terms, extra_conditions=None):
    should = [ must_term_sep(term) for term in terms ]
    must = [{'bool': {'should': should}}]
    if extra_conditions:
        must += extra_conditions
    query = {'query': {'bool': {'must': must }}, '_source': False}
    return query


def make_ids_query(must=None):
    if must:
        query = {'query': {'bool': {'must': must }}, '_source': False}
    else:
        query = {'_source': False}
    return query


def scrollhits(query, size=100):
    import requests
    url = "%s?scroll=1m" % searchurl
    q = dict(query, size=size, sort="_doc")
    print >>sys.stderr, time.time(), "submitting scroll query"
    res = requests.post(url, json=q).json()
    total = res['hits']['total']
    got = len(res['hits']['hits'])
    rem = total - got
    print >>sys.stderr, time.time(), "got %s of %s; %s remaining" % (got, total, rem)
    scroll_id = res['_scroll_id']
    scroll_q = dict(scroll="10m", scroll_id=scroll_id)
    while res['hits']['hits']:
        yield res['hits']['hits']
        if rem == 0:
            break
        res = requests.post(scrollurl, json=scroll_q).json()
        got = len(res['hits']['hits'])
        rem -= got
        print >>sys.stderr, time.time(), "got %s of %s; %s remaining" % (got, total, rem)
    requests.delete(scrollurl, json={'scroll_id': scroll_id})


def scrollids(query, size=100):
    for hits in scrollhits(query, size):
        for hit in hits:
            yield hit['_id']


def get_terms_synonyms(lexpath):
    terms_synonyms = []
    with open(lexpath) as infile:
        for line in infile:
            key, vals = line.strip().split('\t')
            if key == "Concept_ID":
                continue
            terms = vals.strip().split('|')
            #id_ = re.search(r'^[A-Z]+0*(\d+)_', key).group(1)
            terms_synonyms.append((key, terms))
    return terms_synonyms

def term_cleanup(term):
    term = re.sub(r' +', '_', term)
    term = re.sub(r'-',  '_', term)
    term = re.sub(r'\W', '', term)
    return term

def escape(s):
    # sqlite still requires escaping quote characters even when tab separated
    if '\\' in s or '"' in s:
        return re.sub(r'([\\"])', r'\\\1', s)
    else:
        return s

def make_lookup_file(lexpath):
    terms_synonyms = get_terms_synonyms(lexpath)

    for idx, key_terms in enumerate(terms_synonyms, 1):
        key, terms = key_terms
        key_id = key.split('_')[0]
        print "\t".join([str(idx), key_id, key, term_cleanup(terms[0]),
                         '|'.join(map(escape, terms))])


def print_all_ids():
    q = make_ids_query()
    for article_id in scrollids(q, 10000):
        print article_id


def process_file(lexpath, sep=False, verbose=False):
    terms_synonyms = get_terms_synonyms(lexpath)

    for idx, key_terms in enumerate(terms_synonyms, 1):
        key, terms = key_terms
        key_id = key.split('_')[0]
        print >>sys.stderr, time.time(), "%s\t%s" % (idx, key_id)

        if sep:
            q = make_terms_query_sep(terms)
        else:
            q = make_terms_query(terms)
        if verbose:
            print >>sys.stderr, "sending query:", q
        for article_id in scrollids(q, 10000):
            print idx, article_id
        print >>sys.stderr


def usage():
    print "usage: %s [-l|-s] xyz_lexicon.txt" % os.path.basename(__file__)
    print "   or: %s -A" % os.path.basename(__file__)
    sys.exit()


def main(args):
    verbose = False
    if os.environ.get('VERBOSE') == '1':
        verbose = True
    if args[:1] == ['-l']:
        len(args) == 2 or usage()
        make_lookup_file(args[1])
    elif args[:1] == ['-A']:
        print_all_ids()
    elif args[:1] == ['-s']:
        len(args) == 2 or usage()
        process_file(args[1], sep=True, verbose=verbose)
    elif len(args) == 1:
        process_file(args[0], sep=False, verbose=verbose)
    else:
        usage()


if __name__ == '__main__':
    main(sys.argv[1:])

