# -*- coding: utf-8 -*-

import os
import sys
import time
import fcntl
import signal
import logging
import logging.config

from ConfigParser import ConfigParser

from openprocurement.search.version import __version__
from openprocurement.search.engine import IndexEngine, logger
from openprocurement.search.utils import (decode_bool_values, chage_process_user_group,
    setup_watchdog, stop_watchdog)

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

from openprocurement.search.source.asset import AssetSource
from openprocurement.search.index.asset import AssetIndex

from openprocurement.search.source.dgf_lot import DgfLotSource
from openprocurement.search.index.dgf_lot import DgfLotIndex


engine = type('engine', (), {})()


def sigterm_handler(signo, frame):
    logger.info("Signal received %d", signo)
    if engine and hasattr(engine, 'stop_childs'):
        engine.should_exit = True
        engine.stop_childs()
    signal.alarm(10)
    # sys.exit(0)


def force_reindex(config):
    chage_process_user_group(config, logger)
    engine = IndexEngine(config)
    index_key = config['reindex']
    index_prev = index_key + '.prev'
    index_name = engine.get_index(index_key)
    if not index_name:
        print("Error: Can't reindex '%s', current index not found" % index_key)
        return 1
    old_prev = engine.get_index(index_prev) or ''
    if old_prev:
        old_prev = "(old prev %s)" % old_prev
    print("Force reindex %s %s %s" % (index_key, index_name, old_prev))
    engine.set_index(index_prev, index_name)
    engine.set_index(index_key, '')
    return 0


def main():
    if len(sys.argv) < 2 or '--help' in sys.argv:
        print("Usage: index_worker etc/search.ini [option=value] [reindex=index_name]")
        sys.exit(1)

    if '--version' in sys.argv:
        print(__version__)
        sys.exit(0)

    parser = ConfigParser()
    parser.read(sys.argv[1])
    config = dict(parser.items('search_engine'))
    config = decode_bool_values(config)

    for arg in sys.argv[2:]:
        if '=' in arg:
            key, value = arg.split('=', 1)
            config[key] = value

    if 'reindex' in config:
        return force_reindex(config)

    logging.config.fileConfig(sys.argv[1])

    logger.info("Starting ProZorro openprocurement.search.index_worker v%s", __version__)
    logger.info("Copyright (c) 2015-2018 Volodymyr Flonts <flyonts@gmail.com>")

    try:
        chage_process_user_group(config, logger)
    except Exception as e:
        logger.error("Can't change process user: %s", str(e))

    # try get exclusive lock to prevent second start
    lock_filename = config.get('pidfile', '')
    if not lock_filename:
        lock_filename = config.get('index_names', 'index_worker') + '.lock'
    lock_file = open(lock_filename, "w")
    try:
        fcntl.lockf(lock_file, fcntl.LOCK_EX + fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()) + "\n")
        lock_file.flush()
    except Exception as e:
        logger.error("Can't get lock %s maybe already started. %s",
            lock_filename, str(e))
        lock_file.close()
        time.sleep(5)
        return 1

    signal.signal(signal.SIGTERM, sigterm_handler)
    # signal.signal(signal.SIGINT, sigterm_handler)

    if 'watchdog' in config:
        logger.info("Setup watchdog for %s seconds", config['watchdog'])
        setup_watchdog(config['watchdog'], logger)

    try:
        global engine
        engine = IndexEngine(config)
        if config.get('orgs_db', None):
            source = OrgsSource(config, True)
            OrgsIndex(engine, source, config)
        if config.get('tender_api_url', None):
            source = TenderSource(config, True)
            TenderIndex(engine, source, config)
        if config.get('ocds_dir', None):
            source = OcdsSource(config)
            OcdsIndex(engine, source, config)
        if config.get('plan_api_url', None):
            source = PlanSource(config, True)
            PlanIndex(engine, source, config)
        if config.get('auction_api_url', None):
            source = AuctionSource(config, True)
            AuctionIndex(engine, source, config)
        if config.get('auction2_api_url', None):
            source = AuctionSource2(config, True)
            AuctionIndex2(engine, source, config)
        if config.get('asset_api_url', None):
            source = AssetSource(config, True)
            AssetIndex(engine, source, config)
        if config.get('lot_api_url', None):
            source = DgfLotSource(config, True)
            DgfLotIndex(engine, source, config)
        engine.run()
    except Exception as e:
        logger.exception("Unhandled Exception: %s", str(e))
    finally:
        lock_file.close()
        os.remove(lock_filename)
        if engine and hasattr(engine, 'stop_childs'):
            engine.stop_childs()
        if 'watchdog' in config:
            stop_watchdog()
        logger.info("Shutdown")

    return 0


if __name__ == "__main__":
    sys.exit(main())
