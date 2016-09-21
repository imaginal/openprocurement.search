# -*- coding: utf-8 -*-
from datetime import datetime
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class PlanIndex(BaseIndex):
    """OpenProcurement Plans Index
    """
    __index_name__ = 'plans'

    def before_index_item(self, item):
        entity = self.source.procuring_entity(item)
        if entity:
            self.engine.index_by_type('org', entity)

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.index_age() > 120*3600:
            return datetime.now().isoweekday() >= 6
        return False

    def create_index(self, name):
        plan_index = self.config['plan_index']
        logger.info("[%s] Create new plans index from %s",
            name, plan_index)
        with open(plan_index) as f:
            body = json.load(f)
        self.engine.create_index(name, body=body)

