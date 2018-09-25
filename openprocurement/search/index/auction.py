# -*- coding: utf-8 -*-
from datetime import datetime
from openprocurement.search.index import BaseIndex


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

    def create_index(self, name):
        common = 'settings/common_sale.json'
        tender = 'settings/auction.json'
        lang_list = self.config.get('auction_index_lang', '').split(',')
        self.create_tender_index(name, common, tender, lang_list)


class AuctionIndex2(AuctionIndex):
    """OpenProcurement Auction Index
    """
    __index_name__ = 'auctions2'

    def after_init(self):
        self.set_reindex_options(
            self.config.get('auction2_reindex', '5,6'),
            self.config.get('auction2_check', '1,1'))

    def create_index(self, name):
        common = 'settings/common_sale.json'
        tender = 'settings/auction.json'
        lang_list = self.config.get('auction2_index_lang', '').split(',')
        self.create_tender_index(name, common, tender, lang_list)
