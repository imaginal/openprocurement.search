# -*- coding: utf-8 -*-
from time import mktime
from retrying import retry
from iso8601 import parse_date
from socket import setdefaulttimeout
from retrying import retry

from openprocurement_client.client import TendersClient
from openprocurement.search.source import BaseSource

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
        'auction_preload': 500000,
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['auction_limit'] = int(self.config['auction_limit'] or 0) or 100
        self.config['auction_preload'] = int(self.config['auction_preload'] or 0) or 100
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

    @retry(stop_max_attempt_number=5, wait_fixed=15000)
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
            params=params)
        self.skip_until = self.config.get('auction_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None

    def preload(self):
        preload_items = []
        while True:
            try:
                items = self.client.get_tenders()
            except Exception as e:
                logger.error("AuctionSource.preload error %s", str(e))
                self.reset()
                break
            if self.should_exit:
                return []
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

    def get(self, item):
        auction = {}
        retry_count = 0
        while not self.should_exit:
            try:
                auction = self.client.get_tender(item['id'])
                assert auction['data']['id'] == item['id']
                assert auction['data']['dateModified'] >= item['dateModified']
                break
            except Exception as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("get_auction %s retry %d error %s",
                    str(item['id']), retry_count, str(e))
                self.sleep(5)
                if retry_count > 1:
                    self.reset()
        if item['dateModified'] != auction['data']['dateModified']:
            logger.warning("AuctionSource dateModified mismatch %s %s %s",
                item['id'], item['dateModified'],
                auction['data']['dateModified'])
            item['dateModified'] = auction['data']['dateModified']
            item = self.patch_version(item)
        auction['meta'] = item
        return auction
