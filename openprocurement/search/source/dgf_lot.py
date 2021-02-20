# -*- coding: utf-8 -*-
from time import time
from random import random
from datetime import datetime, timedelta

from openprocurement.search.source import BaseSource, TendersClient
from openprocurement.search.utils import long_version, request_error, retry

from logging import getLogger
logger = getLogger(__name__)


class DgfLotSource(BaseSource):
    """DGF Lot Source
    """
    __doc_type__ = 'lot'

    config = {
        'lot_api_key': '',
        'lot_api_url': "",
        'lot_api_version': '0',
        'lot_resource': 'lots',
        'lot_api_mode': '',
        'lot_skip_until': None,
        'lot_skip_after': None,
        'lot_limit': 1000,
        'lot_preload': 5000,
        'lot_fast_client': False,
        'lot_fast_stepsback': 5,
        'lot_reseteach': 0,
        'lot_resethour': 0,
        'lot_user_agent': '',
        'lot_file_cache': '',
        'lot_cache_allow': 'complete,cancelled,unsuccessful',
        'lot_cache_minage': 15,
        'timeout': 30,
    }

    def __init__(self, config={}, use_cache=False):
        if config:
            self.config.update(config)
        self.config['lot_limit'] = int(self.config['lot_limit'] or 100)
        self.config['lot_preload'] = int(self.config['lot_preload'] or 100)
        self.config['lot_reseteach'] = float(self.config['lot_reseteach'] or 0)
        self.config['lot_resethour'] = int(self.config['lot_resethour'] or 0)
        if self.config['lot_reseteach'] > 1:
            self.config['lot_reseteach'] += random()
        self.client_user_agent += " (lots) " + self.config['lot_user_agent']
        if use_cache:
            self.cache_setpath(self.config['lot_file_cache'], self.config['lot_api_url'],
                self.config['lot_api_version'], 'lots')
        if self.cache_path:
            self.cache_allow_status = self.config['lot_cache_allow'].split(',')
            logger.info("[lot] Cache allow status %s", self.cache_allow_status)
        self.fast_client = None
        self.client = None

    def procuring_entity(self, item):
        try:
            return item.data.get('lotCustodian', None)
        except (KeyError, AttributeError):
            return None

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        item['version'] = long_version(item['dateModified'])
        return item

    def patch_lot(self, lot):
        # fix mappings before release of ea2 lots registry 2018-06-26
        # move old-style list of IDs of assets and auctions to separate field
        if lot['data'].get('assets', None) and 'assetsRefs' not in lot['data']:
            if not isinstance(lot['data']['assets'][0], dict):
                lot['data']['assetsRefs'] = lot['data'].pop('assets')
        if lot['data'].get('auctions', None) and 'auctionsRefs' not in lot['data']:
            if not isinstance(lot['data']['auctions'][0], dict):
                lot['data']['auctionsRefs'] = lot['data'].pop('auctions')
        return lot

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time < 3600:
            return False
        if self.config['lot_reseteach'] and time() - self.last_reset_time > 3600 * self.config['lot_reseteach']:
            logger.info("Reset by lot_reseteach=%s", str(self.config['lot_reseteach']))
            return True
        if self.config['lot_resethour'] and datetime.now().hour == int(self.config['lot_resethour']):
            logger.info("Reset by lot_resethour=%s", str(self.config['lot_resethour']))
            return True

    @retry(5, logger=logger)
    def reset(self):
        logger.info("Reset lots client, lot_skip_until=%s lot_skip_after=%s",
                    self.config['lot_skip_until'], self.config['lot_skip_after'])
        self.stat_resets += 1
        params = {}
        if self.config['lot_api_mode']:
            params['mode'] = self.config['lot_api_mode']
        if self.config['lot_limit']:
            params['limit'] = self.config['lot_limit']
        if self.client:
            self.client.close()
        self.client = TendersClient(
            key=self.config['lot_api_key'],
            host_url=self.config['lot_api_url'],
            api_version=self.config['lot_api_version'],
            resource=self.config['lot_resource'],
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        if self.config['lot_file_cache'] and self.cache_path:
            cache_minage = int(self.config['lot_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[lot] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        logger.info("DgfLotClient %s", self.client.cookies)
        if self.fast_client:
            self.fast_client.close()
            self.fast_client = None
        if self.config['lot_fast_stepsback']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['lot_api_key'],
                host_url=self.config['lot_api_url'],
                api_version=self.config['lot_api_version'],
                resource=self.config['lot_resource'],
                params=fast_params,
                session=self.client.session,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + ' (fast_client)')
            for i in range(int(self.config['lot_fast_stepsback'])):
                self.fast_client.get_tenders()
                self.sleep(self.preload_wait)
            self.fast_client.params.pop('descending')
            logger.info("DgfLotClient (fast) %s", self.fast_client.cookies)
        else:
            self.fast_client = None
        self.skip_until = self.config.get('lot_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.skip_after = self.config.get('lot_skip_after', None)
        if self.skip_after and self.skip_after[:2] != '20':
            self.skip_after = None
        self.last_reset_time = time()
        self.should_reset = False

    def preload(self):
        preload_items = []
        # try prelaod last assets first
        if self.fast_client:
            try:
                items = self.fast_client.get_tenders()
                self.stat_queries += 1
                if not len(items):
                    logger.debug("Preload fast 0 lots")
                    raise ValueError()
                preload_items.extend(items)
                logger.info("Preload fast %d assets, last %s",
                    len(preload_items), items[-1]['dateModified'])
            except Exception:
                pass

        retry_count = 0
        while True:
            if retry_count > 3 or self.should_exit:
                break
            try:
                items = self.client.get_tenders()
                self.stat_queries += 1
            except Exception as e:
                retry_count += 1
                logger.error("GET %s retry %d count %d error %s", self.client.prefix_path,
                    retry_count, len(preload_items), request_error(e, self.client))
                self.sleep(5 * retry_count)
                self.reset()
                continue
            if not items:
                break

            preload_items.extend(items)

            if len(items) < 10:
                break
            if len(preload_items) >= self.config['lot_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

        if len(preload_items) >= 100 and items and 'dateModified' in items[-1]:
            logger.info("Preload %d lots, last %s", len(preload_items), items[-1]['dateModified'])

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for lot in self.preload():
            if self.should_exit:
                raise StopIteration()
            if self.skip_until and self.skip_until > lot['dateModified']:
                self.last_skipped = lot['dateModified']
                self.stat_skipped += 1
                continue
            if self.skip_after and self.skip_after < lot['dateModified']:
                self.last_skipped = lot['dateModified']
                self.stat_skipped += 1
                continue
            self.stat_fetched += 1
            yield self.patch_version(lot)

    def cache_allow(self, data):
        if data and data['data']['status'] in self.cache_allow_status:
            return data['data']['dateModified'] < self.cache_allow_dateModified
        return False

    def get(self, item):
        lot = {}
        retry_count = 0
        if self.cache_path:
            lot = self.cache_get(item)
        while not lot:
            if self.should_exit:
                break
            try:
                lot = self.client.get_tender(item['id'])
                assert lot['data']['id'] == item['id'], "lot.id"
                assert lot['data']['dateModified'] >= item['dateModified'], "lot.dateModified"
            except Exception as e:
                if retry_count > 3:
                    raise e
                if self.config.get('ignore_errors', 0) and retry_count > 0:
                    raise e
                retry_count += 1
                logger.error("GET %s/%s meta %s retry %d error %s", self.client.prefix_path,
                    str(item['id']), str(item), retry_count, request_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 2:
                    self.reset()
                lot = {}
            # save to cache
            if lot and self.cache_path:
                self.cache_put(lot)

        if item['dateModified'] != lot['data']['dateModified']:
            logger.debug("Lot dateModified mismatch %s %s %s",
                item['id'], item['dateModified'],
                lot['data']['dateModified'])
            item['dateModified'] = lot['data']['dateModified']
            item = self.patch_version(item)

        lot['meta'] = item
        self.stat_getitem += 1
        return self.patch_lot(lot)
