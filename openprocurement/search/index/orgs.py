# -*- coding: utf-8 -*-
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class OrgsIndex(BaseIndex):
    """org-suggest index
    """
    __index_name__ = 'orgs'

    def need_reindex(self):
        if not self.current_index:
            return True
        return False

    def create_index(self, name):
        orgs_index = self.config['orgs_index']
        logger.debug("Load orgs index settings from %s", orgs_index)
        with open(orgs_index) as f:
            body = json.load(f)
        self.engine.create_index(name, body=body)

