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


class AssetSource(BaseSource):
    """Asset Source
    """
    __doc_type__ = 'asset'

    config = {
        'asset_api_key': '',
        'asset_api_url': "",
        'asset_api_version': '0',
        'asset_resource': 'assets',
        'asset_api_mode': '',
        'asset_skip_until': None,
        'asset_limit': 1000,
        'asset_preload': 10000,
        'asset_reseteach': 3,
        'asset_resethour': 23,
        'asset_user_agent': '',
        'asset_file_cache': '',
        'asset_cache_allow': 'complete,cancelled,unsuccessful',
        'asset_cache_minage': 15,
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['asset_limit'] = int(self.config['asset_limit'] or 100)
        self.config['asset_preload'] = int(self.config['asset_preload'] or 100)
        self.config['asset_reseteach'] = int(self.config['asset_reseteach'] or 3)
        self.config['asset_resethour'] = int(self.config['asset_resethour'] or 0)
        self.client_user_agent += " (assets) " + self.config['asset_user_agent']
        self.cache_setpath(self.config['asset_file_cache'], self.config['asset_api_url'],
            self.config['asset_api_version'], 'assets')
        if self.cache_path:
            self.cache_allow_status = self.config['asset_cache_allow'].split(',')
            logger.info("[asset] Cache allow status %s", self.cache_allow_status)
        self.client = None

    def procuring_entity(self, item):
        try:
            return item.data.get('assetCustodian', None)
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

    def patch_asset(self, asset):
        return asset

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time > 3600 * int(self.config['asset_reseteach']):
            return True
        if time() - self.last_reset_time > 3600:
            return datetime.now().hour == int(self.config['asset_resethour'])

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset assets, asset_skip_until=%s",
                    self.config['asset_skip_until'])
        self.stat_resets += 1
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        params = {}
        if self.config['asset_api_mode']:
            params['mode'] = self.config['asset_api_mode']
        if self.config['asset_limit']:
            params['limit'] = self.config['asset_limit']
        self.client = TendersClient(
            key=self.config['asset_api_key'],
            host_url=self.config['asset_api_url'],
            api_version=self.config['asset_api_version'],
            resource=self.config['asset_resource'],
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        if self.config['asset_file_cache']:
            cache_minage = int(self.config['asset_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[asset] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        logger.info("AssetClient %s", self.client.headers)
        self.skip_until = self.config.get('asset_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
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

            if len(preload_items) >= 100:
                logger.info("Preload %d assets, last %s",
                    len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                break
            if len(preload_items) >= self.config['asset_preload']:
                break

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for asset in self.preload():
            if self.should_exit:
                raise StopIteration()
            if self.skip_until > asset['dateModified']:
                self.last_skipped = asset['dateModified']
                self.stat_skipped += 1
                continue
            self.stat_fetched += 1
            yield self.patch_version(asset)

    def cache_allow(self, data):
        if data and data['data']['status'] in self.cache_allow_status:
            return data['data']['dateModified'] < self.cache_allow_dateModified
        return False

    def get(self, item):
        asset = {}
        retry_count = 0
        if self.cache_path:
            asset = self.cache_get(item)
        while not asset:
            if self.should_exit:
                break
            try:
                asset = self.client.get_tender(item['id'])
                assert asset['data']['id'] == item['id'], "asset.id"
                assert asset['data']['dateModified'] >= item['dateModified'], "asset.dateModified"
            except Exception as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("GET %s/%s retry %d error %s", self.client.prefix_path,
                    str(item['id']), retry_count, restkit_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
                    self.reset()
                asset = {}
            # save to cache
            if asset and self.cache_path:
                self.cache_put(asset)

        if item['dateModified'] != asset['data']['dateModified']:
            logger.debug("Asset dateModified mismatch %s %s %s",
                item['id'], item['dateModified'],
                asset['data']['dateModified'])
            item['dateModified'] = asset['data']['dateModified']
            item = self.patch_version(item)
        asset['meta'] = item
        self.stat_getitem += 1
        return self.patch_asset(asset)
