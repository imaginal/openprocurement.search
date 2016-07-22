# -*- coding: utf-8 -*-
from datetime import datetime
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class OcdsIndex(BaseIndex):
    """OCDS-Tender Index
    """
    __index_name__ = 'ocds'

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.index_age() > 120*3600:
            # TODO: make index_hours configurable
            dt = datetime.now()
            return dt.isoweekday() >= 6 and dt.hour < 6
        return False

    def create_index(self, name):
        ocds_index = self.config['ocds_index']
        logger.debug("Load ocds index settings from %s", ocds_index)
        with open(ocds_index) as f:
            body = json.load(f)
        self.engine.create_index(name, body=body)

    def finish_index(self, name):
        # TODO: create EDRPOU json
        return
