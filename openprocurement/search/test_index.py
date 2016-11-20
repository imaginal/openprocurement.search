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

logger = logging.getLogger()



def main():
    parser = ConfigParser()
    for arg in sys.argv[1:]:
        if arg.startswith('-'):
            continue
        parser.read(arg)

    engine_config = parser.items('search_engine')
    engine = IndexEngine(engine_config)
    source = TenderSource(engine_config)
    index = TenderIndex(engine, source, engine_config)

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    index_names = engine.index_names_dict()

    for k in ['tenders', 'tenders.next', 'tenders.prev']:
        if k in index_names:
            index.check_index(index_names[k])

    return 0

if __name__ == '__main__':
    sys.exit(main())
