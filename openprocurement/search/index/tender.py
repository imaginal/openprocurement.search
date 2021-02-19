# -*- coding: utf-8 -*-
from datetime import datetime
from openprocurement.search.index import BaseIndex


class TenderIndex(BaseIndex):
    """OpenProcurement Tenders Index
    """
    __index_name__ = 'tenders'

    allow_async_reindex = True
    plugin_config_key = 'tender_plugins'

    def after_init(self):
        self.set_reindex_options(
            self.config.get('tender_reindex', '5,6'),
            self.config.get('tender_check', '1,1'))
        self.set_optimize_options(
            self.config.get('tender_optimize', False))
        if self.config.get('tender_save_noindex', False):
            self.noindex_prefix = 'noindex_'

    def need_reindex(self):
        if not self.current_index:
            return True
        if self.force_next_reindex:
            self.force_next_reindex = False
            return True
        if self.index_age() > self.max_age:
            return datetime.now().isoweekday() >= self.reindex_day
        return False

    def before_index_item(self, item):
        entity = self.source.procuring_entity(item)
        if entity:
            self.engine.index_by_type('org', entity)
        return True

    def test_exists(self, index_name, info):
        res = self.engine.test_exists(index_name, info)
        if res is None and self.noindex_prefix:
            index_name = self.noindex_prefix + index_name
            res = self.engine.test_exists(index_name, info)
        return res

    def test_noindex(self, item):
        # noindex filter by procurementMethodType should working
        # only for tenders created since 2016-05-31
        if item.data.tenderID > 'UA-2016-05-31':
            proc_type = item.data.procurementMethodType
            if proc_type == 'negotiation' or proc_type == 'negotiation.quick':
                active = 0
                # updated Saturday, April 14th 2018, by Vasyl Zadvornyy
                # add cancelled award status and check for complaints
                for award in item.data.get('awards', []):
                    if award.get('status', '') in ('active', 'cancelled'):
                        active += 1
                        break
                    if award.get('complaints', []):
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

    def create_index(self, name):
        common = 'settings/common.json'
        tender = 'settings/tender.json'
        lang_list = self.config.get('tender_index_lang', '').split(',')
        self.create_tender_index(name, common, tender, lang_list)
        if self.noindex_prefix:
            noindex_name = self.noindex_prefix + name
            self.create_tender_index(noindex_name, common, tender, lang_list)
