# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import os
import sys
import fcntl
import logging.config

from ConfigParser import ConfigParser

from openprocurement.search.engine import IndexEngine

from openprocurement.search.source.tender import TenderSource
from openprocurement.search.source.ocds import OcdsSource

from openprocurement.search.index.tender import TenderIndex
from openprocurement.search.index.ocds import OcdsIndex

from openprocurement.search.source.plan import PlanSource
from openprocurement.search.index.plan import PlanIndex


def main():
    if len(sys.argv) < 2:
        print("usage: index_worker etc/search.ini")
        sys.exit(1)

    parser = ConfigParser()
    parser.read(sys.argv[1])
    config = dict(parser.items('search_engine'))

    logging.config.fileConfig(sys.argv[1])

    # try get exclusive lock to prevent second start
    lock_filename = config.get('indexer_lock') or 'index_worker.pid'
    lock_file = open(lock_filename, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX+fcntl.LOCK_NB)
    lock_file.write(str(os.getpid())+"\n")
    lock_file.flush()

    try:
        engine = IndexEngine(config)
        if config.get('api_url', None):
            source = TenderSource(config)
            TenderIndex(engine, source, config)
        if config.get('ocds_dir', None):
            source = OcdsSource(config)
            OcdsIndex(engine, source, config)
        if config.get('plan_api_url', None):
            source = PlanSource(config)
            PlanIndex(engine, source, config)
        engine.run()
    finally:
        lock_file.close()
        os.remove(lock_filename)

    return 0


if __name__ == "__main__":
    main()
