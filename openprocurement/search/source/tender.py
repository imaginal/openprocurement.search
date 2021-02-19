# -*- coding: utf-8 -*-
from time import time
from random import random
from datetime import datetime, timedelta

from openprocurement.search.source import BaseSource, TendersClient
from openprocurement.search.source.orgs import OrgsDecoder
from openprocurement.search.utils import long_version, retry, request_error

from logging import getLogger
logger = getLogger(__name__)


class TenderSource(BaseSource):
    """Tenders Source from open openprocurement.API
    """
    __doc_type__ = 'tender'

    config = {
        'tender_api_key': '',
        'tender_api_url': "",
        'tender_api_version': '0',
        'tender_resource': 'tenders',
        'tender_api_mode': '',
        'tender_skip_after': None,
        'tender_skip_until': None,
        'tender_limit': 1000,
        'tender_preload': 5000,
        'tender_reseteach': 11,
        'tender_resethour': 0,
        'tender_optimize': False,
        'tender_bids_tenderers': False,
        'tender_decode_orgs': False,
        'tender_save_noindex': False,
        'tender_fast_client': 0,
        'tender_fast_stepsback': 10,
        'tender_user_agent': '',
        'tender_file_cache': '',
        'tender_cache_allow': 'complete,cancelled,unsuccessful',
        'tender_cache_minage': 15,
        'timeout': 30,
    }

    def __init__(self, config={}, use_cache=False):
        if config:
            self.config.update(config)
        self.config['tender_limit'] = int(self.config['tender_limit'] or 0) or 100
        self.config['tender_preload'] = int(self.config['tender_preload'] or 0) or 100
        self.config['tender_reseteach'] = float(self.config['tender_reseteach'] or 0)
        self.config['tender_resethour'] = int(self.config['tender_resethour'] or 0)
        if self.config['tender_reseteach'] > 1:
            self.config['tender_reseteach'] += random()
        self.client_user_agent += " (tenders) " + self.config['tender_user_agent']
        if use_cache:
            self.cache_setpath(self.config['tender_file_cache'], self.config['tender_api_url'],
                self.config['tender_api_version'], 'tenders')
        if self.cache_path:
            self.cache_allow_status = self.config['tender_cache_allow'].split(',')
            logger.info("[tender] Cache allow status %s", self.cache_allow_status)
        self.preload_wait = 1.0 / float(config.get('query_speed', 100))
        self.fast_client = None
        self.client = None
        self.orgs_db = None

    def procuring_entity(self, item):
        return item.data.get('procuringEntity', None)

    def bids_tenderers(self, item):
        if not self.config['tender_bids_tenderers']:
            return
        if item.data.get('bids', None):
            for bid in item.data['bids']:
                if bid.get('tenderers', None):
                    for tenderer in bid['tenderers']:
                        tenderer_copy = tenderer.copy()
                        tenderer_copy['tenderer'] = 1
                        yield tenderer_copy

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        item['version'] = long_version(item['dateModified'])
        return item

    def patch_tender(self, tender):
        if 'date' not in tender['data']:
            tenderID = tender['data']['tenderID']
            pos = tenderID.find('-20')
            tender['data']['date'] = tenderID[pos+1:pos+11]
        if 'awards' in tender['data']:
            for award in tender['data']['awards']:
                if award.get('status') == 'active':
                    award['activeDate'] = award.get('date')
                    if award.get('suppliers'):
                        for supplier in award['suppliers']:
                            if supplier.get('identifier'):
                                supplier['identifier']['active'] = supplier['identifier'].get('id')
        if 'contracts' in tender['data']:
            for contract in tender['data']['contracts']:
                if contract.get('status') == 'active':
                    contract['activeDate'] = contract.get('date')
                    if contract.get('suppliers'):
                        for supplier in contract['suppliers']:
                            if supplier.get('identifier'):
                                supplier['identifier']['active'] = supplier['identifier'].get('id')

        # TODO: fix failed to parse [agreements.contracts.suppliers.contactPoint.telephone]

        # decode official org name from EDRPOU registry
        if self.config['tender_decode_orgs'] and self.orgs_db:
            if 'procuringEntity' in tender['data']:
                self.orgs_db.patch_entity(tender['data']['procuringEntity'])
            if 'bids' in tender['data']:
                for bid in tender['data']['bids']:
                    if 'tenderers' in bid:
                        for tenderer in bid['tenderers']:
                            self.orgs_db.patch_entity(tenderer)
        return tender

    def need_reset(self):
        if self.should_reset:
            return True
        if self.last_preload_count >= 50 or time() - self.last_reset_time < 3600:
            return False
        if self.config['tender_reseteach'] and (time() - self.last_reset_time > 3600 * self.config['tender_reseteach']):
            logger.info("Reset by tender_reseteach=%s", str(self.config['tender_reseteach']))
            return True
        if self.config['tender_resethour'] and (datetime.now().hour == int(self.config['tender_resethour'])):
            logger.info("Reset by tender_resethour=%s", str(self.config['tender_resethour']))
            return True

    @retry(5, logger=logger)
    def reset(self):
        logger.info("Reset tenders client, tender_skip_until=%s tender_skip_after=%s tender_fast_client=%s",
                    self.config['tender_skip_until'], self.config['tender_skip_after'], self.config['tender_fast_client'])
        self.stat_resets += 1
        if self.config['tender_decode_orgs']:
            self.orgs_db = OrgsDecoder(self.config)
        params = {}
        if self.config['tender_api_mode']:
            params['mode'] = self.config['tender_api_mode']
        if self.config['tender_limit']:
            params['limit'] = self.config['tender_limit']
        if self.client:
            self.client.close()
            self.client = None
        self.client = TendersClient(
            key=self.config['tender_api_key'],
            host_url=self.config['tender_api_url'],
            api_version=self.config['tender_api_version'],
            resource=self.config['tender_resource'],
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        logger.info("TendersClient params %s/api/%s %s",
            self.config['tender_api_url'], self.config['tender_api_version'], self.client.params)
        logger.info("TendersClient cookie %s", self.client.cookies)
        if self.fast_client:
            self.fast_client.close()
            self.fast_client = None
        if str(self.config['tender_fast_client']).strip() == "2":
            # main client from present to future
            self.client.params['descending'] = 1
            self.client.get_tenders()
            self.client.params.pop('descending')
            # self.client.get_tenders()
            # fast client from present to past
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['tender_api_key'],
                host_url=self.config['tender_api_url'],
                api_version=self.config['tender_api_version'],
                resource=self.config['tender_resource'],
                params=fast_params,
                session=self.client.session,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + " back_client")
            logger.info("TendersClient (back) params %s/api/%s %s",
                self.config['tender_api_url'], self.config['tender_api_version'], self.fast_client.params)
            logger.info("TendersClient (back) cookie %s", self.fast_client.cookies)
        elif self.config['tender_fast_client']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['tender_api_key'],
                host_url=self.config['tender_api_url'],
                api_version=self.config['tender_api_version'],
                resource=self.config['tender_resource'],
                params=fast_params,
                session=self.client.session,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + " fast_client")
            for i in range(int(self.config['tender_fast_stepsback'])):
                self.fast_client.get_tenders()
                self.sleep(self.preload_wait)
            self.fast_client.params.pop('descending')
            logger.info("TendersClient (fast) params %s/api/%s %s",
                self.config['tender_api_url'], self.config['tender_api_version'], self.fast_client.params)
            logger.info("TendersClient (fast) cookie %s", self.fast_client.cookies)
        else:
            self.fast_client = None
        if self.config['tender_file_cache'] and self.cache_path:
            cache_minage = int(self.config['tender_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[tender] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        self.skip_until = self.config.get('tender_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.skip_after = self.config.get('tender_skip_after', None)
        if self.skip_after and self.skip_after[:2] != '20':
            self.skip_after = None
        self.last_reset_time = time()
        self.last_preload_count = 0
        self.should_reset = False

    def preload(self):
        preload_items = []
        # try prelaod last tenders first
        retry_count = 0
        while self.fast_client:
            if retry_count > 3 or self.should_exit:
                break
            try:
                items = self.fast_client.get_tenders()
                self.stat_queries += 1
            except Exception as e:
                retry_count += 1
                logger.error("GET %s retry %d count %d error %s", self.client.prefix_path,
                    retry_count, len(preload_items), request_error(e, self.fast_client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
                    self.reset()
                continue
            if not items:
                break

            preload_items.extend(items)

            if len(items) >= 10 and 'dateModified' in items[-1]:
                logger.info("Preload %d tenders, last %s", len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                break
            if len(preload_items) >= self.config['tender_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

        retry_count = 0
        while True:
            if retry_count > 3 or self.should_exit:
                break
            try:
                items = self.client.get_tenders()
                self.stat_queries += 1
            except Exception as e:
                retry_count += 1
                logger.error("GET %s retry %d count %d error %s", self.client.prefix_path,
                    retry_count, len(preload_items), request_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
                    self.reset()
                continue
            if not items:
                break

            preload_items.extend(items)

            if len(items) >= 10 and 'dateModified' in items[-1]:
                logger.info("Preload %d tenders, last %s", len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                break
            if len(preload_items) >= self.config['tender_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

        if not preload_items and self.fast_client:
            if 'descending' in self.fast_client.params:
                self.fast_client.params.pop('offset', '')
            else:
                self.fast_client = None

        self.last_preload_count = len(preload_items)

        return preload_items

    def items(self):
        if not self.client:
            self.reset()

        while not self.should_exit:
            self.last_skipped = None
            self.last_yielded = None

            for tender in self.preload():
                if self.should_exit:
                    raise StopIteration()
                if self.skip_until and self.skip_until > tender['dateModified']:
                    self.last_skipped = tender['dateModified']
                    self.stat_skipped += 1
                    continue
                if self.skip_after and self.skip_after < tender['dateModified']:
                    self.last_skipped = tender['dateModified']
                    self.stat_skipped += 1
                    continue
                self.last_yielded = tender['dateModified']
                self.stat_fetched += 1
                yield self.patch_version(tender)

            if self.last_yielded or not self.last_skipped:
                break

    def cache_allow(self, data):
        if data and data['data']['status'] in self.cache_allow_status:
            return data['data']['dateModified'] < self.cache_allow_dateModified
        return False

    def get(self, item):
        tender = {}
        retry_count = 0
        if self.cache_path:
            tender = self.cache_get(item)
        while not tender:
            if self.should_exit:
                break
            try:
                tender = self.client.get_tender(item['id'])
                assert tender['data']['id'] == item['id'], "bad tender.id"
                assert tender['data']['dateModified'] >= item['dateModified'], "bad tender.dateModified"
            except Exception as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("GET %s/%s meta %s retry %d error %s", self.client.prefix_path,
                    str(item['id']), str(item), retry_count, request_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
                    self.reset()
                tender = {}
            # save to cache
            if tender and self.cache_path:
                self.cache_put(tender)

        if item['dateModified'] != tender['data']['dateModified']:
            if not item.pop('ignore_dateModified', False):
                logger.debug("[tender] dateModified mismatch %s %s %s",
                    item['id'], item['dateModified'],
                    tender['data']['dateModified'])
            item['dateModified'] = tender['data']['dateModified']
            item = self.patch_version(item)
        tender['meta'] = item
        self.stat_getitem += 1
        return self.patch_tender(tender)
