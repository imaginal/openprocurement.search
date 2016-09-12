# -*- coding: utf-8 -*-
from datetime import datetime
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class OcdsIndex(BaseIndex):
    """OCDS old-tender Index
    """
    __index_name__ = 'ocds'

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
        orgs_index = self.config['orgs_index']
        logger.debug("Load orgs index settings from %s", orgs_index)
        with open(orgs_index) as f:
            body = json.load(f)
        self.engine.create_index(name, body=body)
