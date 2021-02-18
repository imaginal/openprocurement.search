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


class AuctionSource(BaseSource):
    """Auction Source
    """
    __doc_type__ = 'auction'

    config = {
        'auction_api_key': '',
        'auction_api_url': "",
        'auction_api_version': '0',
        'auction_resource': 'auctions',
        'auction_api_mode': '',
        'auction_skip_until': None,
        'auction_skip_after': None,
        'auction_limit': 1000,
        'auction_preload': 5000,
        'auction_fast_client': 0,
        'auction_fast_stepsback': 5,
        'auction_reseteach': 0,
        'auction_resethour': 0,
        'auction_user_agent': '',
        'auction_file_cache': '',
        'auction_cache_allow': 'complete,cancelled,unsuccessful',
        'auction_cache_minage': 15,
        'timeout': 30,
    }

    def __init__(self, config={}, use_cache=False):
        if config:
            self.config.update(config)
        self.config['auction_limit'] = int(self.config['auction_limit'] or 100)
        self.config['auction_preload'] = int(self.config['auction_preload'] or 100)
        self.config['auction_reseteach'] = float(self.config['auction_reseteach'] or 0)
        if self.config['auction_reseteach'] > 1:
            self.config['auction_reseteach'] += random()
        self.config['auction_resethour'] = int(self.config['auction_resethour'] or 0)
        self.client_user_agent += " (auctions) " + self.config['auction_user_agent']
        if use_cache:
            self.cache_setpath(self.config['auction_file_cache'], self.config['auction_api_url'],
                self.config['auction_api_version'], 'auctions')
        if self.cache_path:
            self.cache_allow_status = self.config['auction_cache_allow'].split(',')
            logger.info("[auction] Cache allow status %s", self.cache_allow_status)
        self.fast_client = None
        self.client = None

    def procuring_entity(self, item):
        try:
            return item.data.get('procuringEntity', None)
        except (KeyError, AttributeError):
            return None

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        item['version'] = long_version(item['dateModified'])
        return item

    def patch_auction(self, auction):
        if 'date' not in auction['data']:
            auctionID = auction['data']['auctionID']
            pos = auctionID.find('-20')
            auction['data']['date'] = auctionID[pos+1:pos+11]
        if 'items' in auction['data']:
            for item in auction['data']['items']:
                if 'unit' in item and 'quantity' in item:
                    key = 'quantity_' + item['unit']['code']
                    item[key] = item['quantity']
        return auction

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time < 3600:
            return False
        if self.config['auction_reseteach'] and time() - self.last_reset_time > 3600 * self.config['auction_reseteach']:
            logger.info("Reset by auction_reseteach=%s", str(self.config['auction_reseteach']))
            return True
        if self.config['auction_resethour'] and datetime.now().hour == int(self.config['auction_resethour']):
            logger.info("Reset by auction_resethour=%s", str(self.config['auction_resethour']))
            return True

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset auctions client, auction_skip_until=%s auction_skip_after=%s",
                    self.config['auction_skip_until'], self.config['auction_skip_after'])
        self.stat_resets += 1
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        params = {}
        if self.config['auction_api_mode']:
            params['mode'] = self.config['auction_api_mode']
        if self.config['auction_limit']:
            params['limit'] = self.config['auction_limit']
        self.client = TendersClient(
            key=self.config['auction_api_key'],
            host_url=self.config['auction_api_url'],
            api_version=self.config['auction_api_version'],
            resource=self.config['auction_resource'],
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        if self.config['auction_file_cache'] and self.cache_path:
            cache_minage = int(self.config['auction_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[auction2] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        logger.info("AuctionClient %s", self.client.headers)
        if str(self.config['auction_fast_client']) == "2":
            # main client from present to future
            self.client.params['descending'] = 1
            self.client.get_tenders()
            self.client.params.pop('descending')
            # self.client.get_tenders()
            # fast client from present to past
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['auction_api_key'],
                host_url=self.config['auction_api_url'],
                api_version=self.config['auction_api_version'],
                resource=self.config['auction_resource'],
                params=fast_params,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + ' (back_client)')
            logger.info("AuctionClient (back) %s", self.fast_client.headers)
        elif self.config['auction_fast_client']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['auction_api_key'],
                host_url=self.config['auction_api_url'],
                api_version=self.config['auction_api_version'],
                resource=self.config['auction_resource'],
                params=fast_params,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + ' (fast_client)')
            for i in range(int(self.config['auction_fast_stepsback'])):
                self.fast_client.get_tenders()
                self.sleep(self.preload_wait)
            self.fast_client.params.pop('descending')
            logger.info("AuctionClient (fast) %s", self.fast_client.headers)
        else:
            self.fast_client = None
        self.skip_until = self.config.get('auction_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.skip_after = self.config.get('auction_skip_after', None)
        if self.skip_after and self.skip_after[:2] != '20':
            self.skip_after = None
        self.last_reset_time = time()
        self.should_reset = False

    def preload(self):
        preload_items = []
        # try prelaod last auctions first
        while self.fast_client:
            if retry_count > 3 or self.should_exit:
                break
            try:
                items = self.fast_client.get_tenders()
                self.stat_queries += 1
            except Exception as e:
                retry_count += 1
                logger.error("GET %s retry %d count %d error %s", self.client.prefix_path,
                    retry_count, len(preload_items), restkit_error(e, self.fast_client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
                    self.reset()
                continue
            if not items:
                break

            preload_items.extend(items)

            if len(items) >= 10 and 'dateModified' in items[-1]:
                logger.info("Preload %d auctions, last %s", len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                break
            if len(preload_items) >= self.config['auction_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

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
                if retry_count > 1:
                    self.reset()
                continue
            if not items:
                break

            preload_items.extend(items)

            if len(items) >= 10 and 'dateModified' in items[-1]:
                logger.info("Preload %d auctions, last %s", len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                break
            if len(preload_items) >= self.config['auction_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

        if not preload_items and self.fast_client:
            if 'descending' in self.fast_client.params:
                self.fast_client.params.pop('offset', '')
            else:
                self.fast_client = None

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for auction in self.preload():
            if self.should_exit:
                raise StopIteration()
            if self.skip_until and self.skip_until > auction['dateModified']:
                self.last_skipped = auction['dateModified']
                self.stat_skipped += 1
                continue
            if self.skip_after and self.skip_after < auction['dateModified']:
                self.last_skipped = auction['dateModified']
                self.stat_skipped += 1
                continue
            self.stat_fetched += 1
            yield self.patch_version(auction)

    def cache_allow(self, data):
        if data and data['data']['status'] in self.cache_allow_status:
            return data['data']['dateModified'] < self.cache_allow_dateModified
        return False

    def get(self, item):
        auction = {}
        retry_count = 0
        if self.cache_path:
            auction = self.cache_get(item)
        while not auction:
            if self.should_exit:
                break
            try:
                auction = self.client.get_tender(item['id'])
                assert auction['data']['id'] == item['id'], "auction.id"
                assert auction['data']['dateModified'] >= item['dateModified'], "auction.dateModified"
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
                auction = {}
            # save to cache
            if auction and self.cache_path:
                self.cache_put(auction)

        if item['dateModified'] != auction['data']['dateModified']:
            logger.debug("Auction dateModified mismatch %s %s %s",
                item['id'], item['dateModified'],
                auction['data']['dateModified'])
            item['dateModified'] = auction['data']['dateModified']
            item = self.patch_version(item)
        auction['meta'] = item
        self.stat_getitem += 1
        return self.patch_auction(auction)


class AuctionSource2(AuctionSource):
    """Auction Source
    """
    __doc_type__ = 'auction'

    config = {
        'auction2_api_key': '',
        'auction2_api_url': "",
        'auction2_api_version': '0',
        'auction2_resource': 'auctions',
        'auction2_api_mode': '',
        'auction2_skip_until': None,
        'auction2_skip_after': None,
        'auction2_limit': 1000,
        'auction2_preload': 10000,
        'auction2_fast_client': False,
        'auction2_fast_stepsback': 5,
        'auction2_reseteach': 0,
        'auction2_resethour': 0,
        'auction2_user_agent': '',
        'auction2_file_cache': '',
        'auction2_cache_allow': 'complete,cancelled,unsuccessful',
        'auction2_cache_minage': 15,
        'auction_preload': 10000,  # FIXME
        'timeout': 30,
    }

    def __init__(self, config={}, use_cache=False):
        if config:
            self.config.update(config)
        self.config['auction2_limit'] = int(self.config['auction2_limit'] or 100)
        self.config['auction2_preload'] = int(self.config['auction2_preload'] or 100)
        self.config['auction2_reseteach'] = int(self.config['auction2_reseteach'] or 0)
        self.config['auction2_resethour'] = int(self.config['auction2_resethour'] or 0)
        if self.config['auction2_reseteach']:
            self.config['auction2_reseteach'] += random()
        self.config['auction_preload'] = int(self.config['auction2_preload'] or 100)  # FIXME
        self.client_user_agent += " (auctions) " + self.config['auction2_user_agent']
        if use_cache:
            self.cache_setpath(self.config['auction2_file_cache'], self.config['auction2_api_url'],
                self.config['auction2_api_version'], 'auctions')
        if self.cache_path:
            self.cache_allow_status = self.config['auction2_cache_allow'].split(',')
            logger.info("[auction2] Cache allow status %s", self.cache_allow_status)
        self.fast_client = None
        self.client = None

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time < 3600:
            return False
        if self.config['auction2_reseteach'] and time() - self.last_reset_time > 3600 * self.config['auction2_reseteach']:
            return True
        if self.config['auction2_resethour'] and datetime.now().hour == int(self.config['auction2_resethour']):
            return True

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset auctions2, auction2_skip_until=%s auction2_skip_after=%s",
                    self.config['auction2_skip_until'], self.config['auction2_skip_after'])
        self.stat_resets += 1
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        params = {}
        if self.config['auction2_api_mode']:
            params['mode'] = self.config['auction2_api_mode']
        if self.config['auction2_limit']:
            params['limit'] = self.config['auction2_limit']
        self.client = TendersClient(
            key=self.config['auction2_api_key'],
            host_url=self.config['auction2_api_url'],
            api_version=self.config['auction2_api_version'],
            resource=self.config['auction2_resource'],
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        if self.config['auction2_file_cache'] and self.cache_path:
            cache_minage = int(self.config['auction2_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[auction2] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        logger.info("Auction2Client %s", self.client.headers)
        if str(self.config['auction2_fast_client']) == "2":
            # main client from present to future
            self.client.params['descending'] = 1
            self.client.get_tenders()
            self.client.params.pop('descending')
            # self.client.get_tenders()
            # fast client from present to past
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['auction2_api_key'],
                host_url=self.config['auction2_api_url'],
                api_version=self.config['auction2_api_version'],
                resource=self.config['auction2_resource'],
                params=fast_params,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + ' (back_client)')
            logger.info("Auction2Client (back) %s", self.fast_client.headers)
        elif self.config['auction2_fast_client']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['auction2_api_key'],
                host_url=self.config['auction2_api_url'],
                api_version=self.config['auction2_api_version'],
                resource=self.config['auction2_resource'],
                params=fast_params,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + ' (fast_client)')
            for i in range(int(self.config['auction2_fast_stepsback'])):
                self.fast_client.get_tenders()
                self.sleep(self.preload_wait)
            self.fast_client.params.pop('descending')
            logger.info("Auction2Client (fast) %s", self.fast_client.headers)
        else:
            self.fast_client = None
        self.skip_until = self.config.get('auction2_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.skip_after = self.config.get('auction2_skip_after', None)
        if self.skip_after and self.skip_after[:2] != '20':
            self.skip_after = None
        self.last_reset_time = time()
        self.should_reset = False
