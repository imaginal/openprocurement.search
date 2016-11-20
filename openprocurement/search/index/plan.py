# -*- coding: utf-8 -*-
from datetime import datetime
from pkgutil import get_data
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class PlanIndex(BaseIndex):
    """OpenProcurement Plans Index
    """
    __index_name__ = 'plans'

    allow_async_reindex = True

    def after_init(self):
        self.set_reindex_options(
            self.config.get('plan_reindex', '10,7'),
            self.config.get('plan_check', '1,1'))

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.index_age() > self.max_age:
            return datetime.now().isoweekday() >= self.reindex_day
        return False

    def before_index_item(self, item):
        entity = self.source.procuring_entity(item)
        if entity:
            self.engine.index_by_type('org', entity)
        return True

    def create_index(self, name, settings='settings/plan.json'):
        logger.info("Create new index %s from %s", name, settings)
        data = get_data(__name__, settings)
        body = json.loads(data)
        self.engine.create_index(name, body=body)
