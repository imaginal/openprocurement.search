# -*- coding: utf-8 -*-
from time import time, mktime
from datetime import datetime, timedelta
from retrying import retry
from iso8601 import parse_date
from socket import setdefaulttimeout

from openprocurement.search.source import BaseSource, TendersClient
from openprocurement.search.utils import restkit_error

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
        'lot_preload': 10000,
        'lot_reseteach': 3,
        'lot_resethour': 23,
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
        self.config['lot_reseteach'] = int(self.config['lot_reseteach'] or 3)
        self.config['lot_resethour'] = int(self.config['lot_resethour'] or 0)
        self.client_user_agent += " (lots) " + self.config['lot_user_agent']
        if use_cache:
            self.cache_setpath(self.config['lot_file_cache'], self.config['lot_api_url'],
                self.config['lot_api_version'], 'lots')
        if self.cache_path:
            self.cache_allow_status = self.config['lot_cache_allow'].split(',')
            logger.info("[lot] Cache allow status %s", self.cache_allow_status)
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
        dt = parse_date(item['dateModified'])
        version = 1e6 * mktime(dt.timetuple()) + dt.microsecond
        item['version'] = long(version)
        return item

    def patch_lot(self, lot):
        return lot

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time > 3600 * int(self.config['lot_reseteach']):
            return True
        if time() - self.last_reset_time > 3600:
            return datetime.now().hour == int(self.config['lot_resethour'])

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset lots, lot_skip_until=%s lot_skip_after=%s",
                    self.config['lot_skip_until'], self.config['lot_skip_after'])
        self.stat_resets += 1
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        params = {}
        if self.config['lot_api_mode']:
            params['mode'] = self.config['lot_api_mode']
        if self.config['lot_limit']:
            params['limit'] = self.config['lot_limit']
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
        logger.info("DgfLotClient %s", self.client.headers)
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
                    retry_count, len(preload_items), restkit_error(e, self.client))
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

        if len(preload_items) >= 100 and 'dateModified' in items[-1]:
            logger.info("Preload %d lots, last %s", len(preload_items), items[-1]['dateModified'][:20])

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
                retry_count += 1
                logger.error("GET %s/%s retry %d error %s", self.client.prefix_path,
                    str(item['id']), retry_count, restkit_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
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
