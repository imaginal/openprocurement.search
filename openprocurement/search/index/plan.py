# -*- coding: utf-8 -*-
from datetime import datetime
from openprocurement.search.index import BaseIndex


class PlanIndex(BaseIndex):
    """OpenProcurement Plans Index
    """
    __index_name__ = 'plans'

    allow_async_reindex = True
    plugin_config_key = 'plan_plugins'

    def after_init(self):
        self.set_reindex_options(
            self.config.get('plan_reindex', '10,7'),
            self.config.get('plan_check', '1,1'))
        self.set_optimize_options(
            self.config.get('plan_optimize', False))

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
        common = 'settings/common.json'
        tender = 'settings/plan.json'
        lang_list = self.config.get('plan_index_lang', '').split(',')
        self.create_tender_index(name, common, tender, lang_list)
