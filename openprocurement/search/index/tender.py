# -*- coding: utf-8 -*-
from datetime import datetime
from pkgutil import get_data
import simplejson as json

from openprocurement.search.index import BaseIndex, logger


class TenderIndex(BaseIndex):
    """OpenProcurement Tenders Index
    """
    __index_name__ = 'tenders'
    
    allow_async_reindex = True

    def after_init(self):
        self.set_reindex_options(
            self.config.get('tender_reindex', '5,6'),
            self.config.get('tender_check', '1,1'))

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

    def test_noindex(self, item):
        # noindex filter by procurementMethodType should working
        # only for tenders created since 2016-05-31
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
            # Julia Dvornyk Monday, 26 sep 2016, Messenger 9:35am
            elif proc_type == 'competitiveDialogueUA.stage2' or \
                    proc_type == 'competitiveDialogueEU.stage2':
                if item.data.status == 'draft.stage2':
                    return True

        return False

    def create_index(self, name, settings='settings/tender.json'):
        logger.info("Create new index %s from %s", name, settings)
        data = get_data(__name__, settings)
        body = json.loads(data)
        self.engine.create_index(name, body=body)
