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

from openprocurement.search.source.auction import AuctionSource
from openprocurement.search.index.auction import AuctionIndex


LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'

logger = logging.getLogger()


class SearchTester(object):
    engine_config = {}
    search_config = {}
    full_test = False
    offset = 30
    ignore = False
    errors = 0

    def __init__(self, engine_config, search_config):
        self.engine_config = dict(engine_config)
        self.search_config = dict(search_config)

        if self.search_config['host'] == '0.0.0.0':
            self.search_config['host'] = '127.0.0.1'

        self.engine = IndexEngine(self.engine_config)

    def do_search(self, query, tender):
        path = query % tender
        path = 'http://%s:%s/%s' % (
            self.search_config['host'],
            self.search_config['port'], path)
        logger.debug("GET %s", path)
        req = urllib2.urlopen(path, timeout=10)
        code = req.getcode()
        resp = req.read()
        if code != 200:
            raise RuntimeError("%s - HTTP %d" % (path, code))
        data = json.loads(resp)
        if 'items' not in data:
            raise RuntimeError("%s %s - BAD RESP" % (path, tender.id))
        item = {'id': None}
        for item in data['items']:
            if item['id'] == tender['id']:
                break
        if item['id'] != tender['id']:
            raise RuntimeError("%s %s %s - NOT FOUND" % (path, tender.id,
                tender['dateModified']))
        if item['dateModified'] != tender['dateModified']:
            raise RuntimeError("%s %s search=%s api=%s - dateModified mismatch" % (
                path, tender.id, item['dateModified'], tender['dateModified']))
        logger.info("Search %s found %s", path, item['dateModified'])

    def test_tenders(self):
        # self.engine_config['tender_skip_until'] = ''
        self.engine_config['tender_fast_client'] = False

        source = TenderSource(self.engine_config)
        index = TenderIndex(self.engine, source, self.engine_config)

        offset = datetime.now() - timedelta(minutes=self.offset)
        offset = offset.isoformat()

        if self.full_test:
            test_limit = 5000
            skip_limit = 100
            preload = 500000
            limit = 1000
        else:
            test_limit = 5
            skip_limit = 0
            preload = 100
            limit = 100

        source.client_user_agent += " test_search"
        source.reset()
        source.client.params.update({'descending': 1, 'limit': limit})
        source.config['tender_preload'] = preload

        logger.info("Client %s/api/%s mode=%s",
            source.config['tender_api_url'],
            source.config['tender_api_version'],
            source.client.params.get('mode', ''))
        logger.info("Offset %s (%s minutes)",
            offset, self.offset)
        logger.info("Search %s:%s",
            self.search_config['host'],
            self.search_config['port'])

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
                try:
                    tender = source.get(meta)
                    if index.test_noindex(tender):
                        continue
                    if tender.data.dateModified > offset:
                        continue
                    self.do_search('tenders?tid=%(tenderID)s', tender.data)
                except Exception as e:
                    if self.ignore:
                        logger.error("%s (ignored)", e)
                        self.errors += 1
                    else:
                        raise
                test_count += 1
                skip_count = 0
                if test_count >= test_limit:
                    break
            if not meta:
                break

        if test_count < 5:
            raise RuntimeError("Not enough queries")

    def test_plans(self):
        # self.engine_config['plan_skip_until'] = ''
        self.engine_config['plan_fast_client'] = False

        source = PlanSource(self.engine_config)
        index = PlanIndex(self.engine, source, self.engine_config)

        offset = datetime.now() - timedelta(minutes=self.offset)
        offset = offset.isoformat()

        if self.full_test:
            test_limit = 5000
            skip_limit = 100
            preload = 500000
            limit = 1000
        else:
            test_limit = 5
            skip_limit = 0
            preload = 100
            limit = 100

        source.client_user_agent += " test_search"
        source.reset()
        source.client.params.update({'descending': 1, 'limit': limit})
        source.config['plan_preload'] = preload
        source.skip_until = None

        logger.info("Client %s/api/%s mode=%s",
            source.config['plan_api_url'],
            source.config['plan_api_version'],
            source.client.params.get('mode', ''))
        logger.info("Offset %s (%s minutes)",
            offset, self.offset)
        logger.info("Search %s:%s",
            self.search_config['host'],
            self.search_config['port'])

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
                try:
                    tender = source.get(meta)
                    if index.test_noindex(tender):
                        continue
                    if tender.data.dateModified > offset:
                        continue
                    self.do_search('plans?pid=%(planID)s', tender.data)
                except Exception as e:
                    if self.ignore:
                        logger.error("%s (ignored)", e)
                        self.errors += 1
                    else:
                        raise
                test_count += 1
                skip_count = 0
                if test_count >= test_limit:
                    break
            if not meta:
                break

        if test_count < 5:
            raise RuntimeError("Not enough queries")

    def test_auctions(self):
        # self.engine_config['auction_skip_until'] = ''
        self.engine_config['auction_fast_client'] = False

        source = AuctionSource(self.engine_config)
        index = AuctionIndex(self.engine, source, self.engine_config)

        offset = datetime.now() - timedelta(minutes=self.offset)
        offset = offset.isoformat()

        if self.full_test:
            test_limit = 5000
            skip_limit = 100
            preload = 500000
            limit = 1000
        else:
            test_limit = 5
            skip_limit = 0
            preload = 100
            limit = 100

        source.client_user_agent += " test_search"
        source.reset()
        source.client.params.update({'descending': 1, 'limit': limit})
        source.config['auction_preload'] = preload
        source.skip_until = None

        logger.info("Client %s/api/%s mode=%s",
            source.config['auction_api_url'],
            source.config['auction_api_version'],
            source.client.params.get('mode', ''))
        logger.info("Offset %s (%s minutes)",
            offset, self.offset)
        logger.info("Search %s:%s",
            self.search_config['host'],
            self.search_config['port'])

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
                try:
                    tender = source.get(meta)
                    if index.test_noindex(tender):
                        continue
                    if tender.data.dateModified > offset:
                        continue
                    self.do_search('auctions?aid=%(auctionID)s', tender.data)
                except Exception as e:
                    if self.ignore:
                        logger.error("%s (ignored)", e)
                        self.errors += 1
                    else:
                        raise
                test_count += 1
                skip_count = 0
                if test_count >= test_limit:
                    break
            if not meta:
                break

        if test_count < 5:
            raise RuntimeError("Not enough queries")

    def test(self):
        if self.engine_config.get('tender_api_url', None):
            self.test_tenders()

        if self.engine_config.get('plan_api_url', None):
            self.test_plans()

        if self.engine_config.get('auction_api_url', None):
            self.test_auctions()


def print_usage():
    print("Usage: test_search etc/search.ini [options]")
    print("Options are:")
    print("\t--offset=MINUTES\n\t\t- document age to test (default=30)")
    print("\t-d\t- enable debug mode")
    print("\t-f\t- search 1% of all")
    print("\t-i\t- ignore errors")
    print("\t-nt\t- don't test tenders")
    print("\t-np\t- don't test plans")
    print("\t-na\t- don't test auctions")
    print("\t-q\t- be quiet")


def main():
    if len(sys.argv) < 2 or '-h' in sys.argv:
        print_usage()
        sys.exit(1)

    offset = None
    parser = ConfigParser()
    for arg in sys.argv[1:]:
        if arg.startswith('--offset='):
            _, offset = arg.split('=', 1)
            continue
        if arg.startswith('-'):
            continue
        parser.read(arg)

    tester = SearchTester(
        parser.items('search_engine'),
        parser.items('server:main'))
    if offset:
        tester.offset = int(offset)

    log_level = logging.INFO
    if '-d' in sys.argv:
        log_level = logging.DEBUG
    if '-q' in sys.argv:
        log_level = logging.ERROR
    if '-f' in sys.argv:
        tester.full_test = True
    if '-i' in sys.argv:
        tester.ignore = True
    if '-nt' in sys.argv:
        tester.engine_config['tender_api_url'] = None
    if '-np' in sys.argv:
        tester.engine_config['plan_api_url'] = None
    if '-na' in sys.argv:
        tester.engine_config['auction_api_url'] = None

    logging.basicConfig(level=log_level, format=LOG_FORMAT)

    try:
        tester.test()
    except KeyboardInterrupt:
        logger.info("User interrupt")
        pass
    except Exception as e:
        if '-d' in sys.argv:
            logger.exception("%s", e)
        logger.error("%s", e)
        return 1

    if tester.errors:
        logger.warning("Total %d error(s)", tester.errors)
        return 2

    logger.info("Test passed.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
