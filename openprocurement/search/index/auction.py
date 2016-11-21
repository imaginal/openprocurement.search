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

    def after_init(self):
        self.set_reindex_options(
            self.config.get('auction_reindex', '5,6'),
            self.config.get('auction_check', '1,1'))

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.force_next_reindex:
            self.force_next_reindex = False
            return True
        if self.index_age() > self.max_age:
            return datetime.now().isoweekday() >= self.reindex_day
        return False

    def before_index_item(self, item):
        entity = self.source.procuring_entity(item)
        if entity:
            self.engine.index_by_type('org', entity)
        return True

    def create_index(self, name, settings='settings/auction.json'):
        logger.info("Create new index %s from %s", name, settings)
        data = get_data(__name__, settings)
        body = json.loads(data)
        for key in self.index_settings_keys:
            body['settings']['index'][key] = self.config[key]
        self.engine.create_index(name, body=body)
