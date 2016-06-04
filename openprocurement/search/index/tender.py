# -*- coding: utf-8 -*-
from datetime import datetime
import simplejson as json

from openprocurement.search.index import BaseIndex

class TenderIndex(BaseIndex):
    """OpenProcurement Tenders Index
    """
    __index_name__ = 'tenders'

    def test_noindex(self, item):
        # don't index by procurementMethodType should working
        # for tenders created since 2016-05-31
        if item.data.tenderID > 'UA-2016-05-31':
            proc_type = item.data.procurementMethodType
            if proc_type == 'negotiation' or proc_type == 'negotiation.quick':
                active = 0
                for award in item.data.get('awards', []):
                    if award.get('status', '') == 'active':
                        active += 1
                        break
                if active == 0:
                    return True
            elif proc_type == 'reporting':
                active = 0
                for contract in item.data.get('contracts', []):
                    if contract.get('status', '') == 'active':
                        active += 1
                        break
                if active == 0:
                    return True

        return False

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.index_age() > 72*3600:
            # TODO: make index_hours configurable
            dt = datetime.now()
            return dt.weekday() > 5 and dt.hour < 5
        return False

    def create_index(self, name):
        body = None
        try:
            tender_index = self.config['tender_index']
            if tender_index:
                with open(tender_index) as f:
                    body = json.load(f)
        except (KeyError, ValueError):
            pass
        self.engine.create_index(name, body=body)

    def finish_index(self, name):
        # TODO: create EDRPOU json
        return
