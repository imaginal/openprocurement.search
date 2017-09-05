# -*- coding: utf-8 -*-
from datetime import datetime
from openprocurement.search.index import BaseIndex


class AssetIndex(BaseIndex):
    """OpenProcurement Asset Index
    """
    __index_name__ = 'assets'

    allow_async_reindex = True

    def after_init(self):
        self.set_reindex_options(
            self.config.get('asset_reindex', '5,6'),
            self.config.get('asset_check', '1,1'))

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
        common = 'settings/common_asset.json'
        tender = 'settings/asset.json'
        lang_list = self.config.get('asset_index_lang', '').split(',')
        self.create_tender_index(name, common, tender, lang_list)
