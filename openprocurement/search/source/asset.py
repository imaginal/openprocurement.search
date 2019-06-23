# -*- coding: utf-8 -*-
from time import time
from random import random
from datetime import datetime, timedelta
from retrying import retry
from socket import setdefaulttimeout

from openprocurement.search.source import BaseSource, TendersClient
from openprocurement.search.utils import long_version, restkit_error

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
        'asset_skip_after': None,
        'asset_limit': 1000,
        'asset_preload': 5000,
        'asset_fast_client': False,
        'asset_fast_stepsback': 5,
        'asset_reseteach': 0,
        'asset_resethour': 0,
        'asset_user_agent': '',
        'asset_file_cache': '',
        'asset_cache_allow': 'complete,cancelled,unsuccessful',
        'asset_cache_minage': 15,
        'timeout': 30,
    }

    def __init__(self, config={}, use_cache=False):
        if config:
            self.config.update(config)
        self.config['asset_limit'] = int(self.config['asset_limit'] or 100)
        self.config['asset_preload'] = int(self.config['asset_preload'] or 100)
        self.config['asset_reseteach'] = int(self.config['asset_reseteach'] or 0)
        self.config['asset_resethour'] = int(self.config['asset_resethour'] or 0)
        if self.config['asset_reseteach'] > 1:
            self.config['asset_reseteach'] += random()
        self.client_user_agent += " (assets) " + self.config['asset_user_agent']
        if use_cache:
            self.cache_setpath(self.config['asset_file_cache'], self.config['asset_api_url'],
                self.config['asset_api_version'], 'assets')
        if self.cache_path:
            self.cache_allow_status = self.config['asset_cache_allow'].split(',')
            logger.info("[asset] Cache allow status %s", self.cache_allow_status)
        self.fast_client = None
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
        item['version'] = long_version(item['dateModified'])
        return item

    def patch_asset(self, asset):
        return asset

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time > 3600 * self.config['asset_reseteach']:
            return True
        if time() - self.last_reset_time > 3600:
            return datetime.now().hour == int(self.config['asset_resethour'])

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset assets client, asset_skip_until=%s asset_skip_after=%s",
                    self.config['asset_skip_until'], self.config['asset_skip_after'])
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
        if self.config['asset_file_cache'] and self.cache_path:
            cache_minage = int(self.config['asset_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[asset] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        logger.info("AssetClient %s", self.client.headers)
        if self.config['asset_fast_client']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['asset_api_key'],
                host_url=self.config['asset_api_url'],
                api_version=self.config['asset_api_version'],
                resource=self.config['asset_resource'],
                params=fast_params,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + ' (fast_client)')
            for i in range(int(self.config['asset_fast_stepsback'])):
                self.fast_client.get_tenders()
                self.sleep(self.preload_wait)
            self.fast_client.params.pop('descending')
            logger.info("AssetClient (fast) %s", self.fast_client.headers)
        else:
            self.fast_client = None
        self.skip_until = self.config.get('asset_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.skip_after = self.config.get('asset_skip_after', None)
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
                    logger.debug("Preload fast 0 assets")
                    raise ValueError()
                preload_items.extend(items)
                logger.info("Preload fast %d assets, last %s",
                    len(preload_items), items[-1]['dateModified'])
            except:
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
                    retry_count, len(preload_items), restkit_error(e, self.client))
                self.sleep(5 * retry_count)
                self.reset()
                continue
            if not items:
                break

            preload_items.extend(items)

            if len(items) < 10:
                break
            if len(preload_items) >= self.config['asset_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

        if len(preload_items) >= 100 and items and 'dateModified' in items[-1]:
            logger.info("Preload %d assets, last %s", len(preload_items), items[-1]['dateModified'])

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for asset in self.preload():
            if self.should_exit:
                raise StopIteration()
            if self.skip_until and self.skip_until > asset['dateModified']:
                self.last_skipped = asset['dateModified']
                self.stat_skipped += 1
                continue
            if self.skip_after and self.skip_after < asset['dateModified']:
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
                if self.config.get('ignore_errors', 0) and retry_count > 0:
                    raise e
                retry_count += 1
                logger.error("GET %s/%s meta %s retry %d error %s", self.client.prefix_path,
                    str(item['id']), str(item), retry_count, restkit_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 2:
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
