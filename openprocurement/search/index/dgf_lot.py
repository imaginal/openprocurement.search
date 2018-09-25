# -*- coding: utf-8 -*-
from datetime import datetime
from openprocurement.search.index import BaseIndex


class DgfLotIndex(BaseIndex):
    """OpenProcurement DGF Lots Index
    """
    __index_name__ = 'lots'

    allow_async_reindex = True

    def after_init(self):
        self.set_reindex_options(
            self.config.get('lot_reindex', '5,6'),
            self.config.get('lot_check', '1,1'))

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
        dgflot = 'settings/dgf_lot.json'
        lang_list = self.config.get('lot_index_lang', '').split(',')
        self.create_tender_index(name, common, dgflot, lang_list)
