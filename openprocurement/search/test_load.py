#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import simplejson as json
import urllib, urllib2
from datetime import datetime, timedelta
from random import choice, randint
from multiprocessing import Process
from time import time, sleep

g_dict = {}
g_args = None
logger = logging.getLogger('test_load')
FORMAT = '%(asctime)-15s %(levelname)s %(processName)s %(message)s'


def worker():
    logger.debug('Starting process')

    base_url = g_args.api_url[0]
    if base_url.find('/') < 0:
        base_url = '127.0.0.1:8484/' + base_url
    if base_url.find('://') < 0:
        base_url = 'http://' + base_url
    plan_mode = base_url.endswith('plans')

    max_errs = g_args.e
    max_notf = g_args.z
    max_reqs = g_args.n

    requests = 0
    n_errors = 0
    n_notfnd = 0

    start_time = time()

    while requests < max_reqs:
        requests += 1
        key = choice(g_dict.keys())
        args = list()
        # full text query
        if key == 'query':
            word = ''
            while len(word) < 3:
                code = choice(g_dict[key].keys())
                name = g_dict[key][code]
                word = choice(name.split(' '))
                word = word.replace('"', '').encode('utf-8')
            args.append((key, word))
        elif key == 'cpv' or key == 'dkpp':
            code = choice(g_dict[key].keys())
            like = 5 if key == 'dkpp' else 6
            if len(code) > like:
                code = code[:like]
            if plan_mode:
                key = 'plan_'+key
            key += '_like'
            args.append((key, code))
        elif key == 'date':
            subkey = choice(g_dict[key].keys())
            start = datetime.now()-timedelta(days=randint(10,60))
            start = start.isoformat()[:10]
            end = datetime.now()+timedelta(days=randint(1,30))
            end = end.isoformat()[:10]
            args.append((subkey+'_start', start))
            args.append((subkey+'_end', end))
        elif key == 'tid' or key == 'pid':
            code = choice(g_dict[key].keys())
            key += '_like'
            args.append((key, code))
        else:
            code = choice(g_dict[key].keys())
            args.append((key, code))
        qs = urllib.urlencode(args, True)
        url = base_url + '?' + qs
        try:
            req = urllib2.urlopen(url, timeout=g_args.t)
            code = req.getcode()
            resp = req.read()
            if code == 200:
                data = json.loads(resp)
                items = data['items']
                total = data['total']
                if not items or not total:
                    n_notfnd += 1
                logger.debug("%d %d %s total %d", code, len(resp), url, total)
            else:
                logging.error("%d %d %s", code, len(resp), url)
                n_errors += 1
        except Exception as e:
            logging.error('Exception %s on %s', str(e), url)
            n_errors += 1
        if n_notfnd >= max_notf:
            logging.error('Exit by max not_found reached (%d requests)', requests)
            return
        if n_errors >= max_errs:
            logging.error('Exit by max error occurred (%d requests)', requests)
            return

    total_time = time() - start_time
    query_rate = 1.0 * requests / total_time

    logger.info('Leaving process, %d requests, %d not found, %d errors, %1.1f r/s',
                 requests, n_notfnd, n_errors, query_rate)


def load_json(filename):
    if not filename:
        return {}
    with open(filename) as f:
        data = json.load(f)
    return data


def prepare():
    global g_args, g_dict
    parser = argparse.ArgumentParser(description='openprocurement.search.test_load')
    parser.add_argument('-c', metavar='concurrency', type=int, default=1)
    parser.add_argument('-e', metavar='max_errors', type=int, default=10)
    parser.add_argument('-z', metavar='max_not_found', type=int, default=100)
    parser.add_argument('-n', metavar='requests', type=int, default=100)
    parser.add_argument('-t', metavar='timeout', type=int, default=10)
    parser.add_argument('-v', metavar='verbosity', help='[10,20,30,40]',
        type=int, default=logging.INFO)
    parser.add_argument('--log', metavar='output.log', nargs=1)
    parser.add_argument('--tid', metavar='tenderID.json', nargs=1)
    parser.add_argument('--pid', metavar='planID.json', nargs=1)
    parser.add_argument('--cpv', metavar='cpv.json', nargs=1)
    parser.add_argument('--dkpp', metavar='dkpp.json', nargs=1)
    parser.add_argument('--date', metavar='date.json', nargs=1)
    parser.add_argument('--edrpou', metavar='edrpou.json', nargs=1)
    parser.add_argument('--region', metavar='region.json', nargs=1)
    parser.add_argument('--status', metavar='status.json', nargs=1)
    parser.add_argument('--query', metavar='query.json', nargs=1)
    parser.add_argument('api_url', metavar='http://api.host[:port]/resource', nargs=1)
    g_args = parser.parse_args()

    log_kw = {'level': g_args.v, 'format': FORMAT}
    if g_args.log:
        log_kw['filename'] = g_args.log[0]
    logging.basicConfig(**log_kw)

    for key in ['tid', 'pid', 'cpv', 'dkpp', 'date', 'edrpou', 'region', 'status', 'query']:
        args_list = getattr(g_args, key, None)
        if isinstance(args_list, list):
            for filename in args_list:
                logger.debug('Load %s from %s', key, filename)
                g_dict[key] = load_json(filename)

    if not g_dict:
        raise ValueError('At least one of cpv, dkpp, edrpou or query is required')


def main():
    prepare()

    logger.info('Starting %d workers', g_args.c)
    process_list = list()
    start_time = time()
    for i in range(g_args.c):
        p = Process(target=worker)
        process_list.append(p)
        p.daemon = True
        p.start()

    logger.info('Waiting for workers')
    for p in process_list:
        p.join()

    total_time = time() - start_time
    query_rate = 1.0 * g_args.n * g_args.c / total_time
    logger.info('Total %d x %d queries in %1.3f sec %1.1f r/s',
        g_args.c, g_args.n, total_time, query_rate)


if __name__ == '__main__':
    main()
