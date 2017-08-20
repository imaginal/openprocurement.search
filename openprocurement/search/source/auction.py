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
        'auction_limit': 1000,
        'auction_preload': 10000,
        'auction_resethour': 23,
        'auction_user_agent': '',
        'auction_file_cache': '',
        'auction_cache_allow': 'complete,cancelled,unsuccessful',
        'auction_cache_minage': 15,
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['auction_limit'] = int(self.config['auction_limit'] or 100)
        self.config['auction_preload'] = int(self.config['auction_preload'] or 100)
        self.config['auction_resethour'] = int(self.config['auction_resethour'] or 0)
        self.client_user_agent += " (auctions) " + self.config['auction_user_agent']
        self.cache_setpath(self.config['auction_file_cache'], self.config['auction_api_url'],
            self.config['auction_api_version'], 'auctions')
        if self.cache_path:
            self.cache_allow_status = self.config['auction_cache_allow'].split(',')
            logger.info("[auction] Cache allow status %s", self.cache_allow_status)
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
        dt = parse_date(item['dateModified'])
        version = 1e6 * mktime(dt.timetuple()) + dt.microsecond
        item['version'] = long(version)
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
        if time() - self.last_reset_time > 3600:
            return datetime.now().hour == int(self.config['auction_resethour'])

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset auctions, auction_skip_until=%s",
                    self.config['auction_skip_until'])
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
        if self.config['auction_file_cache']:
            cache_minage = int(self.config['auction_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[auction2] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        logger.info("AuctionClient %s", self.client.headers)
        self.skip_until = self.config.get('auction_skip_until', None)
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
                logger.info("Preload %d auctions, last %s",
                    len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                break
            if len(preload_items) >= self.config['auction_preload']:
                break

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for auction in self.preload():
            if self.should_exit:
                raise StopIteration()
            if self.skip_until > auction['dateModified']:
                self.last_skipped = auction['dateModified']
                continue
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
                retry_count += 1
                logger.error("GET %s/%s retry %d error %s", self.client.prefix_path,
                    str(item['id']), retry_count, restkit_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
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
        'auction2_limit': 1000,
        'auction2_preload': 10000,
        'auction2_resethour': 23,
        'auction2_user_agent': '',
        'auction2_file_cache': '',
        'auction2_cache_allow': 'complete,cancelled,unsuccessful',
        'auction2_cache_minage': 15,
        'auction_preload': 10000,  # FIXME
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['auction2_limit'] = int(self.config['auction2_limit'] or 100)
        self.config['auction2_preload'] = int(self.config['auction2_preload'] or 100)
        self.config['auction2_resethour'] = int(self.config['auction2_resethour'] or 0)
        self.config['auction_preload'] = int(self.config['auction2_preload'] or 100)  # FIXME
        self.client_user_agent += " (auctions) " + self.config['auction2_user_agent']
        self.cache_setpath(self.config['auction2_file_cache'], self.config['auction2_api_url'],
            self.config['auction2_api_version'], 'auctions')
        if self.cache_path:
            self.cache_allow_status = self.config['auction2_cache_allow'].split(',')
            logger.info("[auction2] Cache allow status %s", self.cache_allow_status)
        self.client = None

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time > 3600:
            return datetime.now().hour == int(self.config['auction2_resethour'])

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset auctions2, auction2_skip_until=%s",
                    self.config['auction2_skip_until'])
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
        if self.config['auction2_file_cache']:
            cache_minage = int(self.config['auction2_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[auction2] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        logger.info("AuctionClient2 %s", self.client.headers)
        self.skip_until = self.config.get('auction2_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.last_reset_time = time()
        self.should_reset = False
