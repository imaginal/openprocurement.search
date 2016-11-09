#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import logging
import simplejson as json
import urllib, urllib2
from time import time, sleep
from datetime import datetime, timedelta
from ConfigParser import ConfigParser

from openprocurement.search.engine import IndexEngine

from openprocurement.search.source.tender import TenderSource
from openprocurement.search.index.tender import TenderIndex

from openprocurement.search.source.plan import PlanSource
from openprocurement.search.index.plan import PlanIndex

LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'
TEST_LIMIT = 5

logger = logging.getLogger()
class search:
    config = {}
    offset = 15


def do_search(query, tender):
    path = query % tender
    url = 'http://%s:%s/%s' % (
        search.config['host'],
        search.config['port'], path)
    req = urllib2.urlopen(url, timeout=10)
    code = req.getcode()
    resp = req.read()
    if code != 200:
        raise RuntimeError("%s - HTTP Error %d" % (path, code))
    data = json.loads(resp)
    if 'items' not in data:
        raise RuntimeError("%s %s - BAD Resp" % (path, tender.id))
    item = {'id': None}
    for item in data['items']:
        if item['id'] == tender['id']:
            break
    if item['id'] != tender['id']:
        raise RuntimeError("%s %s - NOT FOUND api=%s" % (path, tender.id,
            tender['dateModified']))
    if item['dateModified'] != tender['dateModified']:
        raise RuntimeError("%s %s - dateModified mismatch, search=%s api=%s" % (
            path, tender.id, item['dateModified'], tender['dateModified']))
    logger.info("Search %s found %s", path, item['dateModified'])


def test_tenders(engine, config, full_test=False):
    config['skip_until'] = ''
    source = TenderSource(config)
    index = TenderIndex(engine, source, config)

    offset = datetime.now() - timedelta(minutes=search.offset)
    offset = offset.isoformat()

    if full_test:
        test_limit = 5000
        skip_limit = 100
        preload = 500000
        limit = 1000
    else:
        test_limit = 5
        skip_limit = 0
        preload = 100
        limit = 100

    source.reset()
    source.client.params.update({'descending': 1, 'limit': limit})
    source.config['preload'] = preload

    mode = source.client.params.get('mode', None)
    if mode:
        logger.info("TendersClient mode %s", mode)

    test_count = 0
    skip_count = skip_limit

    while test_count < test_limit:
        meta = None
        for meta in source.items():
            if meta.dateModified > offset:
                continue
            if skip_count < skip_limit:
                skip_count += 1
                continue
            tender = source.get(meta)
            if index.test_noindex(tender):
                continue
            if tender.data.dateModified > offset:
                continue
            do_search('tenders?tid=%(tenderID)s', tender.data)
            test_count += 1
            skip_count = 0
            if test_count >= test_limit:
                break
        if not meta:
            break


def test_plans(engine, config, full_test=False):
    config['plan_skip_until'] = ''
    source = PlanSource(config)
    index = PlanIndex(engine, source, config)

    offset = datetime.now() - timedelta(minutes=search.offset)
    offset = offset.isoformat()

    if full_test:
        test_limit = 5000
        skip_limit = 100
        preload = 500000
        limit = 1000
    else:
        test_limit = 5
        skip_limit = 0
        preload = 100
        limit = 100

    source.reset()
    source.client.params.update({'descending': 1, 'limit': limit})
    source.config['plan_preload'] = preload
    source.skip_until = None

    mode = source.client.params.get('mode', None)
    if mode:
        logger.info("PlansClient mode %s", mode)

    test_count = 0
    skip_count = skip_limit

    while test_count < test_limit:
        meta = None
        for meta in source.items():
            if meta.dateModified > offset:
                continue
            if skip_count < skip_limit:
                skip_count += 1
                continue
            tender = source.get(meta)
            if index.test_noindex(tender):
                continue
            if tender.data.dateModified > offset:
                continue
            do_search('plans?pid=%(planID)s', tender.data)
            test_count += 1
            skip_count = 0
            if test_count >= test_limit:
                break
        if not meta:
            break


def main():
    if len(sys.argv) < 2:
        print("Usage: test_search etc/search.ini [options]")
        print("Options are:")
        print("\t-d\t- enable debug mode")
        print("\t-f\t- search 1% of all")
        print("\t-q\t- be quiet")
        sys.exit(1)

    parser = ConfigParser()
    for arg in sys.argv[1:]:
        if arg[:9] == '--offset=':
            search.offset = int(arg[9:])
            continue
        if arg[:1] == '-':
            continue
        parser.read(arg)

    config = dict(parser.items('search_engine'))
    search.config = dict(parser.items('server:main'))
    if search.config['host'] == '0.0.0.0':
        search.config['host'] = '127.0.0.1'

    engine = IndexEngine(config)

    log_level = logging.INFO
    if '-d' in sys.argv:
        log_level = logging.DEBUG
    if '-q' in sys.argv:
        log_level = logging.ERROR
    full_test = False
    if '-f' in sys.argv:
        full_test = True

    logging.basicConfig(level=log_level, format=LOG_FORMAT)
    logger.info("Start with config %s", sys.argv[1])

    try:
        if config.get('api_url', None):
            test_tenders(engine, config, full_test)

        if config.get('plan_api_url', None):
            test_plans(engine, config, full_test)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        if '-d' in sys.argv:
            raise
        logger.error("%s", e)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
