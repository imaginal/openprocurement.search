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

from openprocurement.search.source.orgs import OrgsSource
from openprocurement.search.index.orgs import OrgsIndex

from openprocurement.search.source.tender import TenderSource
from openprocurement.search.index.tender import TenderIndex

from openprocurement.search.source.ocds import OcdsSource
from openprocurement.search.index.ocds import OcdsIndex

from openprocurement.search.source.plan import PlanSource
from openprocurement.search.index.plan import PlanIndex

from openprocurement.search.source.auction import AuctionSource, AuctionSource2
from openprocurement.search.index.auction import AuctionIndex, AuctionIndex2

LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'

logger = logging.getLogger(__name__)


class IndexTester(object):
    def __init__(self, config):
        self.config = dict(config)
        self.engine = IndexEngine(self.config)
        self.errors = 0

    def init_engine(self):
        if self.config.get('orgs_db', None):
            source = OrgsSource(self.config)
            OrgsIndex(self.engine, source, self.config)

        if self.config.get('tender_api_url', None):
            source = TenderSource(self.config)
            TenderIndex(self.engine, source, self.config)

        if self.config.get('plan_api_url', None):
            source = PlanSource(self.config)
            PlanIndex(self.engine, source, self.config)

        if self.config.get('ocds_dir', None):
            source = OcdsSource(self.config)
            OcdsIndex(self.engine, source, self.config)

        if self.config.get('auction_api_url', None):
            source = AuctionSource(self.config)
            AuctionIndex(self.engine, source, self.config)

        if self.config.get('auction2_api_url', None):
            source = AuctionSource2(self.config)
            AuctionIndex2(self.engine, source, self.config)

    def test(self):
        self.init_engine()
        self.errors = 0

        for index in self.engine.index_list:
            if not index.current_index:
                logger.warning("No current index for %s", index.__index_name__)
                continue
            if not index.check_index(index.current_index):
                self.errors += 1

        if self.errors:
            raise RuntimeError("%d error while check indexes" % self.errors)


def print_usage():
    print("Usage: test_search etc/search.ini [options]")
    print("Options are:")
    print("\t-d\t- enable debug mode")
    print("\t-q\t- be quiet")


def main():
    if len(sys.argv) < 2 or '-h' in sys.argv:
        print_usage()
        sys.exit(1)

    parser = ConfigParser()
    for arg in sys.argv[1:]:
        if arg.startswith('-'):
            continue
        parser.read(arg)
        break

    if not parser.has_section('search_engine'):
        print("Not a config.file")
        sys.exit(1)

    config = parser.items('search_engine')
    tester = IndexTester(config)

    log_level = logging.INFO
    if '-d' in sys.argv:
        log_level = logging.DEBUG
    if '-q' in sys.argv:
        log_level = logging.ERROR
    # nt - ignore tedners, np - plans, no - ocds, na - auctions
    if '-nt' in sys.argv:
        tester.config['tender_api_url'] = None
    if '-np' in sys.argv:
        tester.config['plan_api_url'] = None
    if '-no' in sys.argv:
        tester.config['ocds_dir'] = None
    if '-na' in sys.argv:
        tester.config['auction_api_url'] = None
    if '-na2' in sys.argv:
        tester.config['auction2_api_url'] = None
    if '-ns' in sys.argv:
        tester.config['orgs_db'] = None

    logging.basicConfig(level=log_level, format=LOG_FORMAT)

    if log_level != logging.DEBUG:
        tracer = logging.getLogger('elasticsearch')
        tracer.setLevel(logging.WARNING)

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

    logger.info("Test passed.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
