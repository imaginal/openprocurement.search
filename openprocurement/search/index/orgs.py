# -*- coding: utf-8 -*-
import simplejson as json
from pkgutil import get_data

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

    def index_item(self, index_name, item):
        try:
            return self.engine.index_item(index_name, item)
        except Exception as e:
            if self.config['ignore_errors']:
                return None
            if not self.test_exists(index_name, item['meta']):
                raise e

    def create_index(self, name, settings='settings/orgs.json'):
        logger.info("Create new index %s from %s", name, settings)
        data = get_data(__name__, settings)
        body = json.loads(data)
        self.engine.create_index(name, body=body)
