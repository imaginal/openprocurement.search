#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import simplejson as json
import urllib, urllib2
from random import choice
from multiprocessing import Process
from time import time

FORMAT='%(asctime)-15s %(levelname)s %(processName)s %(message)s'
g_args=None
g_dict={}


def worker():
    logging.debug('Starting process')

    requests = g_args.n
    while requests > 0:
        requests -= 1
        key = choice(g_dict.keys())
        args = list()
        # full text query
        if key == 'query':
            code = choice(g_dict[key].keys())
            name = g_dict[key][code]
            word = choice(name.split(' '))
            word = word.replace('"', '').encode('utf-8')
            args.append((key, word))
        elif key == 'cpv' or key == 'dkpp':
            code = choice(g_dict[key].keys())
            if len(code) > 8:
                code = code[:8]
            args.append((key, code))
        else:
            code = choice(g_dict[key].keys())
            args.append((key, code))
        qs = urllib.urlencode(args, True)
        url = g_args.host[0] + '/search?' + qs
        try:
            req = urllib2.urlopen(url, timeout=g_args.t)
            code = req.getcode()
            resp = req.read()
            if code == 200:
                data = json.loads(resp)
                logging.debug("%d %d %s total %d", code, len(resp), url, data['total'])
            else:
                logging.error("%d %d %s", code, len(resp), url)
        except Exception as e:
            logging.error('Exception %s on %s', str(e), url)

    logging.debug('Leaving process')


def load_json(filename):
    if not filename:
        return {}
    with open(filename) as f:
        data = json.load(f)
    return data


def prepare():
    global g_args, g_dict
    parser = argparse.ArgumentParser(description='openprocurement.search.test_load')
    parser.add_argument('-c', metavar='concurrency', type=int, default=10)
    parser.add_argument('-n', metavar='requests', type=int, default=100)
    parser.add_argument('-t', metavar='timeout', type=int, default=10)
    parser.add_argument('-v', metavar='verbosity', type=int, default=logging.INFO)
    parser.add_argument('--cpv', metavar='cpv.json', nargs=1)
    parser.add_argument('--dkpp', metavar='dkpp.json', nargs=1)
    parser.add_argument('--edrpou', metavar='edrpou.json', nargs=1)
    parser.add_argument('--region', metavar='region.json', nargs=1)
    parser.add_argument('--status', metavar='status.json', nargs=1)
    parser.add_argument('--query', metavar='query.json', nargs=1)
    parser.add_argument('host', metavar='http://search-api.host[:port]', nargs=1)
    g_args = parser.parse_args()

    logging.basicConfig(level=g_args.v, format=FORMAT)
    for key in ['cpv', 'dkpp', 'edrpou', 'region', 'status', 'query']:
        args_list = getattr(g_args, key, None)
        if isinstance(args_list, list):
            for filename in args_list:
                logging.debug('Load %s from %s', key, filename)
                g_dict[key] = load_json(filename)

    if not g_dict:
        raise ValueError('At least one of cpv, dkpp, edrpou or query is required')


def main():
    start_time = time()
    prepare()

    logging.debug('Starting workers...')
    process_list = list()
    for i in range(g_args.c):
        p = Process(target=worker)
        process_list.append(p)
        p.start()

    logging.debug('Waiting for workers')
    for p in process_list:
        p.join()

    total_time = time() - start_time
    query_rate = 1.0 * g_args.n * g_args.c / total_time
    logging.info('Total %d x %d queries in %1.3f sec. %1.1f r/s',
        g_args.n, g_args.c, total_time, query_rate)


if __name__ == '__main__':
    main()
