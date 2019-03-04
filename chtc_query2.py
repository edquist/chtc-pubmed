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

from requests.auth import HTTPBasicAuth


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
URL_BASE = 'http://localhost:9200/articles_dummy/article/_count'
AUTH = HTTPBasicAuth('dduser', 'searchtime')

def make_query_object(target_term, key_phrase, through_year):
    # build the constraint list first
    should_tt = []
    should_kp = []
    must = []

    # are we searching on a target term
    if target_term is not None:
        target_list = target_term[1]
        for target_tok in target_list:
            should_tt.append({
                'match_phrase' : {'title' : target_tok},
                })
            should_tt.append({
                'match_phrase' : {'abstract' : target_tok}
                })

    # and key phrase
    if key_phrase is not None:
        kp_list = key_phrase[1]
        for kp_tok in kp_list:
            should_kp.append({
                'match_phrase' : {'title' : kp_tok},
                })
            should_kp.append({
                'match_phrase' : {'abstract' : kp_tok}
                })

    # always add the year constraint
    must.append({
        'bool': {
            'should': should_tt
        }
    })
    must.append({
        'bool': {
            'should': should_kp
        }
    })
    must.append({'range' : {'publication_date.year' : {'lte' : through_year}}})
    query = {'query': {'bool': {'must': must } } }

    return query

def get_count(target_term, key_phrase, through_year, url_base, auth):
    assert not isinstance(key_phrase, str) , "my error msg"
    # target term and/or key phrase may be None
    q = make_query_object(target_term, key_phrase, through_year)
    res = requests.get(url_base, data=json.dumps(q), auth=auth)
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

# this is the API version used for the web interface
def run_full_query(all_targets, key_phrase, through_year=None, sep_kp=False):
    # handle through year
    if through_year is None:
        through_year = datetime.datetime.now().year
    # handle separated key phrase
    if sep_kp:
        key_phrase = key_phrase.strip().split()
    # we will return this as a dictionary stored somewhat compactly
    ret = dict()
    # compute the total number of articles in the database
    db_article_cnt = get_count(None, None, through_year, URL_BASE, AUTH)
    ret['db_article_cnt'] = db_article_cnt
    # and the number of times the key phrase shows up
    kp_cnt = get_count(None, key_phrase, through_year, URL_BASE, AUTH)
    ret['kp_cnt'] = kp_cnt
    # now go through each target
    ret['target'] = list()
    ret['targ_cnt'] = list()
    ret['targ_with_kp_cnt'] = list()
    for target in all_targets:
        targ_with_kp_cnt = 0
        # first the individual count
        targ_cnt = get_count(target, None, through_year, URL_BASE, AUTH)
        # only need to query combined if articles exist for both individually
        if targ_cnt > 0 and kp_cnt > 0:
            # now do both key phrase and target
            targ_with_kp_cnt = get_count(target, key_phrase, through_year, URL_BASE, AUTH)
        ret['target'].append(target)
        ret['targ_cnt'].append(targ_cnt)
        ret['targ_with_kp_cnt'].append(targ_with_kp_cnt)
    return ret

def get_terms_asTokens(FILE_NAME):
    terms_asTokens = {}
    with open(FILE_NAME, 'r') as infile:
        for line in infile:
            tmp = line.strip().split('\t')
            if len(tmp) == 1:
                tmp = line.strip().split('   ')
            assert len(tmp)==2, "we expect just id and |-separated string here"
            var = tmp[1].strip().split('|')
            tokens = []
            for el in var:
                t = el.split(' ')
                if len(t) == 0:
                    tokens = [el for el in range(t)]
                else:
                    tokens.extend(t)
            unique_tokens_only = list(set(i.lower() for i in tokens))
            terms_synonyms.update({tmp[0]: unique_tokens_only})
    return terms_asTokens

def get_terms_synonyms(FILE_NAME):
    terms_synonyms = {}
    with open(FILE_NAME, 'r') as infile:
        for line in infile:
            tmp = line.strip().split('\t')
            if len(tmp) == 1:
                tmp = line.strip().split('   ')
            assert len(tmp)==2, "we expect just id and |-separated string here"
            terms_synonyms.update({tmp[0]: tmp[1].strip().split('|')})
    return terms_synonyms

def get_output_filename(CUI_key, kp_synonym_list):
    cuikey = CUI_key.split('_')[0]
    syn = kp_synonym_list[0].replace(' ', '_')
    output_file = cuikey + '_' + syn
    return output_file

def do_junk(key_phrase):
#    print("\tInvestigating key phrase %s" % key_phrase[0])
    outfile_name = get_output_filename(key_phrase[0], key_phrase[1])
    outfile_path_name = os.path.join(OUTPUT_DIRECTORY, outfile_name + ".txt")
    with open(outfile_path_name, 'w') as out_fh:
        kp_cnt = get_count(None, key_phrase, THROUGH_YEAR, URL_BASE, AUTH)

        out_fh.write('target\ttarget_with_keyphrase_count\ttarget_count\tkeyphrase_count\tdb_article_count' + '\n')
        for target in target_terms.items():
            targ_with_kp_cnt = 0
            # first the individual count
            targ_cnt = get_count(target, None, THROUGH_YEAR, URL_BASE, AUTH)

            # only need to query combined if articles exist for both individually
            if targ_cnt > 0 and kp_cnt > 0:
                # now do both key phrase and target
                targ_with_kp_cnt = get_count(target, key_phrase, THROUGH_YEAR, URL_BASE, AUTH)

            key = target[0]
            value = target[1]
            outstr = '{0}\t{1}\t{2}\t{3}\t{4}'.format(key+':'+value[0], targ_with_kp_cnt, targ_cnt, kp_cnt, db_article_cnt)
            #print (outstr)
            out_fh.write(outstr + '\n')



def main():
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
    db_article_cnt = get_count(None, None, THROUGH_YEAR, URL_BASE, AUTH)
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
