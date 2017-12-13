#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import simplejson as json
import urllib
import urllib2
from datetime import datetime, timedelta
from random import choice, randint
from multiprocessing import Process
from time import time

g_dict = {}
g_ordr = {}
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
    auction_mode = base_url.find('auctions') + 1

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
        sort = ''
        order = ''
        while key == 'sort':
            sort = choice(g_dict[key].keys())
            order = choice(['', '', '', 'asc', 'desc'])
            key = choice(g_dict.keys())
        args = list()
        # full text query
        if key == 'query':
            code = choice(g_dict[key].keys())
            query = g_dict[key][code]
            query = query.encode('utf-8')
            args.append((key, query))
        elif key in ('cav', 'cpv', 'cpvs', 'dkpp'):
            code = choice(g_dict[key].keys())
            like = {'dkpp': 5, 'cpv': 6, 'cav': 4, 'cpvs': 4}.get(key, 6)
            if auction_mode:
                like = 4
            if len(code) > like:
                code = code[:like]
            if plan_mode:
                key = 'plan_' + key
            key += '_like'
            args.append((key, code))
        elif key == 'date':
            subkey = choice(g_dict[key].keys())
            start = datetime.now() - timedelta(days=randint(10, 60))
            start = start.isoformat()[:10]
            end = datetime.now() + timedelta(days=randint(1, 30))
            end = end.isoformat()[:10]
            args.append((subkey + '_start', start))
            args.append((subkey + '_end', end))
        elif key == 'tid' or key == 'pid' or key == 'aid':
            code = choice(g_dict[key].keys())
            key += '_like'
            args.append((key, code))
        else:
            code = choice(g_dict[key].keys())
            args.append((key, code))
        if sort:
            args.append(('sort', sort))
        if sort and order:
            args.append(('order', order))
        qs = urllib.urlencode(args, True)
        if '?' in base_url:
            url = base_url + '&' + qs
        else:
            url = base_url + '?' + qs
        code = 0
        resp = ''
        try:
            req = urllib2.urlopen(url, timeout=g_args.t)
            code = req.getcode()
            resp = req.read()
            if not resp:
                resp = ''
            if code != 200:
                raise ValueError("BAD RESPONSE")
            data = json.loads(resp)
            items = data.get('items')
            total = data.get('total')
            error = data.get('error')
            if error:
                raise ValueError(error)
            if not items or not total:
                n_notfnd += 1
            logger.debug("%d %d %s total %d", code, len(resp), url, total)
        except Exception as e:
            logger.error("%d %d %s error %s", code, len(resp), url, str(e))
            n_errors += 1
        if n_notfnd >= max_notf:
            logger.error('Exit by max_not_found reached (%d requests, %d not found)',
                requests, n_notfnd)
            sys.exit(1)
            return
        if n_errors >= max_errs:
            logger.error('Exit by max error occurred (%d requests, %d errors)',
                requests, n_errors)
            sys.exit(2)
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
    parser.add_argument('-v', metavar='verbosity', help='10 = debug, 40 = error',
        type=int, default=logging.INFO)
    parser.add_argument('--log', metavar='output.log', nargs=1)
    parser.add_argument('--aid', metavar='auction_id.json', nargs=1)
    parser.add_argument('--tid', metavar='tender_id.json', nargs=1)
    parser.add_argument('--pid', metavar='plan_id.json', nargs=1)
    parser.add_argument('--cav', metavar='cav.json', nargs=1)
    parser.add_argument('--cpv', metavar='cpv.json', nargs=1)
    parser.add_argument('--cpvs', metavar='cpv.json', nargs=1)
    parser.add_argument('--dkpp', metavar='dkpp.json', nargs=1)
    parser.add_argument('--date', metavar='date.json', nargs=1)
    parser.add_argument('--edrpou', metavar='edrpou.json', nargs=1)
    parser.add_argument('--region', metavar='region.json', nargs=1)
    parser.add_argument('--item_region', metavar='region.json', nargs=1)
    parser.add_argument('--item_square', metavar='square.json', nargs=1)
    parser.add_argument('--value', metavar='value.json', nargs=1)
    parser.add_argument('--budget', metavar='budget.json', nargs=1)
    parser.add_argument('--status', metavar='status.json', nargs=1)
    parser.add_argument('--query', metavar='query.json', nargs=1)
    parser.add_argument('--sort', metavar='sort.json', nargs=1)
    parser.add_argument('api_url', metavar='http://api.host[:port]/resource', nargs=1)
    g_args = parser.parse_args()

    log_kw = {'level': g_args.v, 'format': FORMAT}
    if g_args.log:
        log_kw['filename'] = g_args.log[0]
    logging.basicConfig(**log_kw)

    for key in ['aid', 'tid', 'pid', 'cav', 'cpv', 'cpvs', 'dkpp', 'date', 'edrpou', 'region',
                'item_region', 'item_square', 'value', 'budget', 'status', 'query', 'sort']:
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
    errors = 0
    for p in process_list:
        p.join()
        if p.exitcode:
            errors += 1

    total_time = time() - start_time
    query_rate = 1.0 * g_args.n * g_args.c / total_time
    logger.info('Total %d x %d queries in %1.3f sec %1.1f r/s',
        g_args.c, g_args.n, total_time, query_rate)

    if errors:
        logger.error("But %d of %d childs failed", errors, g_args.c)
        sys.exit(1)

    return 0


if __name__ == '__main__':
    sys.exit(main())
