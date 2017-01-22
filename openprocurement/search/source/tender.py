# -*- coding: utf-8 -*-
#import gevent
from time import time, mktime
from iso8601 import parse_date
from datetime import datetime
from socket import setdefaulttimeout
from retrying import retry

from openprocurement.search.source import BaseSource, TendersClient
from openprocurement.search.source.orgs import OrgsDecoder
from openprocurement.search.utils import restkit_error

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
        'tender_api_mode': '',
        'tender_skip_until': None,
        'tender_limit': 1000,
        'tender_preload': 10000,
        'tender_resethour': 22,
        'tender_decode_orgs': False,
        'tender_fast_client': False,
        'tender_user_agent': '',
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['tender_limit'] = int(self.config['tender_limit'] or 0) or 100
        self.config['tender_preload'] = int(self.config['tender_preload'] or 0) or 100
        self.config['tender_resethour'] = int(self.config['tender_resethour'] or 0)
        self.client_user_agent += " (tenders) " + self.config['tender_user_agent']
        self.fast_client = None
        self.client = None
        self.orgs_db = None

    def procuring_entity(self, item):
        return item.data.get('procuringEntity', None)

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        dt = parse_date(item['dateModified'])
        version = 1e6 * mktime(dt.timetuple()) + dt.microsecond
        item['version'] = long(version)
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
        if 'contracts' in tender['data']:
            for contract in tender['data']['contracts']:
                if contract.get('status') == 'active':
                    contract['activeDate'] = contract.get('date')
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
        if time() - self.last_reset_time > 3600:
            return datetime.now().hour == int(self.config['tender_resethour'])

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset tenders, tender_skip_until=%s", self.config['tender_skip_until'])
        if self.config['tender_decode_orgs']:
            self.orgs_db = OrgsDecoder(self.config)
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        params = {}
        if self.config['tender_api_mode']:
            params['mode'] = self.config['tender_api_mode']
        if self.config['tender_limit']:
            params['limit'] = self.config['tender_limit']
        self.client = TendersClient(
            key=self.config['tender_api_key'],
            host_url=self.config['tender_api_url'],
            api_version=self.config['tender_api_version'],
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        logger.info("TendersClient %s", self.client.headers)
        if self.config['tender_fast_client']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['tender_api_key'],
                host_url=self.config['tender_api_url'],
                api_version=self.config['tender_api_version'],
                params=fast_params,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent+" fast_client")
            self.fast_client.get_tenders()
            self.fast_client.params.pop('descending')
            logger.info("TendersClient (fast) %s", self.fast_client.headers)
        else:
            self.fast_client = None
        self.skip_until = self.config.get('tender_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.last_reset_time = time()
        self.should_reset = False

    def preload(self):
        preload_items = []
        # try prelaod last tenders first
        if self.fast_client:
            try:
                items = self.fast_client.get_tenders()
                if not len(items):
                    raise ValueError()
                preload_items.extend(items)
                logger.info("Preload fast %d tenders, last %s",
                    len(preload_items), items[-1]['dateModified'])
            except:
                pass

        while True:
            try:
                items = self.client.get_tenders()
            except Exception as e:
                logger.error("TenderSource.preload error %s", restkit_error(e, self.client))
                self.reset()
                break
            if self.should_exit:
                return []
            if not items:
                break

            preload_items.extend(items)

            if len(preload_items) >= 100:
                logger.info("Preload %d tenders, last %s",
                    len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                self.fast_client = None
                break
            if len(preload_items) >= self.config['tender_preload']:
                break

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for tender in self.preload():
            if self.should_exit:
                raise StopIteration()
            if self.skip_until > tender['dateModified']:
                self.last_skipped = tender['dateModified']
                continue
            yield self.patch_version(tender)

    def get(self, item):
        tender = {}
        retry_count = 0
        while not self.should_exit:
            try:
                tender = self.client.get_tender(item['id'])
                assert tender['data']['id'] == item['id']
                assert tender['data']['dateModified'] >= item['dateModified']
                break
            except Exception as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("get_tender %s retry %d error %s",
                    str(item['id']), retry_count, restkit_error(e, self.client))
                self.sleep(5)
                if retry_count > 1:
                    self.reset()
        if item['dateModified'] != tender['data']['dateModified']:
            logger.debug("Tender dateModified mismatch %s %s %s",
                item['id'], item['dateModified'],
                tender['data']['dateModified'])
            item['dateModified'] = tender['data']['dateModified']
            item = self.patch_version(item)
        tender['meta'] = item
        return self.patch_tender(tender)
