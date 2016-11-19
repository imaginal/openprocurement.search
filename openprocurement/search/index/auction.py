# -*- coding: utf-8 -*-
from datetime import datetime
from pkgutil import get_data
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class AuctionIndex(BaseIndex):
    """OpenProcurement Auction Index
    """
    __index_name__ = 'auctions'

    allow_async_reindex = True

    def before_index_item(self, item):
        entity = self.source.procuring_entity(item)
        if entity:
            self.engine.index_by_type('org', entity)
        return True

    def after_init(self):
        reindex = self.config.get('auction_reindex', '5,6')
        self.max_age, self.reindex_day = map(int, reindex.split(','))
        self.max_age *= 86400

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.index_age() > self.max_age:
            return datetime.now().isoweekday() >= self.reindex_day
        return False

    def create_index(self, name, settings='settings/auction.json'):
        logger.info("Create new index %s from %s", name, settings)
        data = get_data(__name__, settings)
        body = json.loads(data)
        self.engine.create_index(name, body=body)
