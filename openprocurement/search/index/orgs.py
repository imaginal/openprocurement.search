# -*- coding: utf-8 -*-
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class OrgsIndex(BaseIndex):
    """org-suggest index
    """
    __index_name__ = 'orgs'

    allow_async_reindex = False

    def need_reindex(self):
        if not self.current_index:
            return True
        return False

    def create_index(self, name):
        orgs_index = self.config['orgs_index']
        logger.info("Create new suggest index %s from %s",
                    name, orgs_index)
        with open(orgs_index) as f:
            body = json.load(f)
        self.engine.create_index(name, body=body)

    def index_item(self, index_name, item):
        try:
            return self.engine.index_item(index_name, item)
        except Exception as e:
            if self.config['ignore_errors']:
                return None
            if not self.test_exists(index_name, item['meta']):
                raise e
