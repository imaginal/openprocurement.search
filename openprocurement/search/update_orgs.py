# -*- coding: utf-8 -*-

import os
import sys
import fcntl
import signal
import logging.config
from datetime import datetime, timedelta

from ConfigParser import ConfigParser

from openprocurement.search.version import __version__
from openprocurement.search.engine import IndexEngine, logger

from openprocurement.search.index.orgs import OrgsIndex
from openprocurement.search.source.orgs import OrgsSource
from openprocurement.search.source.tender import TenderSource
from openprocurement.search.source.ocds import OcdsSource
from openprocurement.search.source.plan import PlanSource
from openprocurement.search.source.auction import AuctionSource, AuctionSource2
from openprocurement.search.source.asset import AssetSource
from openprocurement.search.source.dgf_lot import DgfLotSource
from openprocurement.search.utils import decode_bool_values, chage_process_user_group


engine = type('engine', (), {})()


def sigterm_handler(signo, frame):
    logger.warning("Signal received %d", signo)
    if hasattr(engine, 'current_source'):
        engine.current_source.should_exit = True
    engine.should_exit = True
    signal.alarm(2)
    sys.exit(0)


class IndexOrgsEngine(IndexEngine):
    def __init__(self, config, update_config):
        self.patch_engine_config(config, update_config)
        super(IndexOrgsEngine, self).__init__(config)
        self.orgs_map = {}

    def patch_engine_config(self, config, update_config):
        config['slave_mode'] = None
        config['start_wait'] = 0
        config['tender_fast_client'] = False
        config['plan_fast_client'] = False
        config['auction_fast_client'] = False
        config['auction2_fast_client'] = False
        config['lot_fast_client'] = False
        config['asset_fast_client'] = False
        config['tender_preload'] = int(1e6)
        config['plan_preload'] = int(1e6)
        config['ocds_preload'] = int(1e6)
        config['auction_preload'] = int(1e6)
        # update skip_until
        update_days = update_config.get('update_days') or 30
        date = datetime.now() - timedelta(days=int(update_days))
        date = date.strftime("%Y-%m-%d")
        logger.info("Patch config: update_days = %s -> skip_until = %s", update_days, date)
        config['auction_skip_until'] = date
        config['tender_skip_until'] = date
        config['plan_skip_until'] = date
        config['ocds_skip_until'] = date

    def process_entity(self, entity):
        code = None
        try:
            code = entity['identifier']['id']
            if not code:
                raise ValueError("No code")
            if type(code) != str:
                code = str(code)
            if len(code) < 5 or len(code) > 15:
                raise ValueError("Bad code")
        except (KeyError, TypeError, ValueError):
            return False
        try:
            self.index_by_type('org', entity)
        except Exception as e:
            logger.exception("Can't index %s: %s", code, str(e))
        if code in self.orgs_map:
            self.orgs_map[code] += 1
        else:
            self.orgs_map[code] = 1
        return True

    def process_source(self, source):
        logger.info("Process source [%s]", source.doc_type)
        source.client_user_agent += " update_orgs"
        engine.current_source = source
        items_list = True
        items_count = 0
        flush_count = 0
        while True:
            if self.should_exit:
                return
            try:
                save_count = items_count
                items_list = source.items()
                for meta in items_list:
                    if self.should_exit:
                        return
                    items_count += 1
                    item = source.get(meta)
                    entity = source.procuring_entity(item)
                    if entity:
                        self.process_entity(entity)
                    if self.config.get('orgs_from_bids', False):
                        for entity in source.bids_tenderers(item):
                            self.process_entity(entity)
                    # log progress
                    if items_count % 100 == 0:
                        logger.info("Processed %d %ss, last %s orgs_found %d",
                            items_count, source.doc_type,
                            meta.get('dateModified'), len(self.orgs_map))
                    # flush orgs_map each 10k
                    if items_count - flush_count > 10000:
                        flush_count = items_count
                        self.flush_orgs_map()
            except Exception as e:
                logger.exception("Can't process_source: %s", str(e))
                break
            # prevent stop by skip_until before first 100 processed
            if items_count < 100 and getattr(source, 'last_skipped', None):
                logger.info("[%s] Processed %d last_skipped %s",
                    source.doc_type, items_count, source.last_skipped)
                continue
            elif items_count - save_count < 5:
                break
        # flush orgs ranks
        self.flush_orgs_map()

    def flush_orgs_map(self):
        if self.should_exit:
            return
        index_name = self.get_current_indexes()
        logger.info("[%s] Flush orgs to index", index_name)
        if not index_name or not self.orgs_map:
            return
        iter_count = 0
        update_count = 0
        orgs_index = self.index_list[0]
        orgs_index.process(allow_reindex=False)
        doc_type = orgs_index.source.doc_type
        map_len = len(self.orgs_map)
        error_count = 0
        for code, rank in self.orgs_map.iteritems():
            if self.should_exit:
                break
            iter_count += 1
            if iter_count % 1000 == 0:
                logger.info("[%s] Updated %d / %d orgs %d%%",
                    index_name, update_count, iter_count,
                    int(100 * iter_count / map_len))
            # dont update rare companies
            if rank < 10:
                continue
            # get item
            meta = {'id': code, 'doc_type': doc_type}
            found = self.get_item(index_name, meta)
            # if not found - ignore, but warn
            if not found:
                logger.warning("[%s] Code %s not found", index_name, str(code))
                continue
            # if rank not changed - ignore
            if found['_source']['rank'] == rank:
                continue
            item = {
                'meta': {
                    'id': found['_id'],
                    'doc_type': found['_type'],
                    'version': found['_version'] + 1,
                },
                'data': found['_source'],
            }
            item['data']['rank'] = rank
            try:
                self.index_item(index_name, item)
                update_count += 1
            except Exception as e:
                logger.error("Fail index %s: %s", str(item), str(e))
                error_count += 1
                if error_count > 100:
                    logger.exception("%s", str(e))
                    break
        # final info
        logger.info("[%s] Updated %d / %d orgs %d%%",
            index_name, update_count, iter_count,
            int(100.0 * iter_count / map_len))


def main():
    if len(sys.argv) < 2 or '-h' in sys.argv:
        print("Usage: update_orgs etc/search.ini [index_names=custom_index_names] [orgs_from_bids=1]")
        sys.exit(1)

    parser = ConfigParser()
    parser.read(sys.argv[1])
    config = dict(parser.items('search_engine'))
    config = decode_bool_values(config)
    uo_config = dict(parser.items('update_orgs'))

    logging.config.fileConfig(sys.argv[1])

    logger.info("Starting openprocurement.search.update_orgs v%s", __version__)
    logger.info("Copyright (c) 2015-2018 Volodymyr Flonts <flyonts@gmail.com>")

    # try get exclusive lock to prevent second start
    lock_filename = uo_config.get('pidfile') or 'update_orgs.pid'
    lock_file = open(lock_filename, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX + fcntl.LOCK_NB)
    lock_file.write(str(os.getpid()) + "\n")
    lock_file.flush()

    for arg in sys.argv[2:]:
        if '=' in arg:
            key, value = arg.split('=', 1)
            config[key] = value
            logger.info("Update config %s=%s", key, value)

    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGINT, sigterm_handler)

    try:
        chage_process_user_group(config)
    except Exception as e:
        logger.error("Can't change process user: %s", str(e))

    try:
        global engine

        engine = IndexOrgsEngine(config, uo_config)
        source = OrgsSource(config)
        index = OrgsIndex(engine, source, config)
        # manualy reset and prevent second reset on first process_source
        source.reset()
        index.last_current_index = index.current_index
        if config.get('tender_api_url', None):
            source = TenderSource(config)
            engine.process_source(source)
        if config.get('ocds_dir', None):
            source = OcdsSource(config)
            engine.process_source(source)
        if config.get('plan_api_url', None):
            source = PlanSource(config)
            engine.process_source(source)
        if config.get('auction_api_url', None):
            source = AuctionSource(config)
            engine.process_source(source)
        if config.get('auction2_api_url', None):
            source = AuctionSource2(config)
            engine.process_source(source)
        if config.get('asset_api_url', None):
            source = AssetSource(config)
            engine.process_source(source)
        if config.get('lot_api_url', None):
            source = DgfLotSource(config)
            engine.process_source(source)
        engine.flush_orgs_map()
    except KeyboardInterrupt:
        return 1
    except Exception as e:
        logger.exception("Exception: %s", str(e))
    finally:
        lock_file.close()
        os.remove(lock_filename)
        logger.info("Shutdown")

    return 0


if __name__ == "__main__":
    main()
