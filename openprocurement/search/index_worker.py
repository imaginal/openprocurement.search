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
from openprocurement.search.index.tender import TenderIndex

LOCK_FILE = "index_worker.lock"


def main():
    if len(sys.argv) < 2:
        print("usage: index_worker etc/search.ini")
        sys.exit(1)

    parser = ConfigParser()
    parser.read(sys.argv[1])
    config = dict(parser.items('search_engine'))

    logging.config.fileConfig(sys.argv[1])

    # try get exclusive lock to prevent second start
    lock_file = open(LOCK_FILE, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX+fcntl.LOCK_NB)
    lock_file.write(str(os.getpid())+"\n")
    lock_file.flush()

    try:
        engine = IndexEngine(config)
        source = TenderSource(config)
        TenderIndex(engine, source, config)
        engine.run()
    finally:
        lock_file.close()
        os.remove(LOCK_FILE)

    return 0


if __name__ == "__main__":
    main()
