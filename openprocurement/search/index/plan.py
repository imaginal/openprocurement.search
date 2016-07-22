# -*- coding: utf-8 -*-
from datetime import datetime
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class PlanIndex(BaseIndex):
    """OpenProcurement Plans Index
    """
    __index_name__ = 'plans'

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.index_age() > 120*3600:
            # TODO: make index_hours configurable
            dt = datetime.now()
            return dt.isoweekday() >= 6 and dt.hour < 6
        return False

    def create_index(self, name):
        plan_index = self.config['plan_index']
        logger.debug("Load plan index settings from %s", plan_index)
        with open(plan_index) as f:
            body = json.load(f)
        self.engine.create_index(name, body=body)

    def finish_index(self, name):
        # TODO: create EDRPOU json
        return
