# -*- coding: utf-8 -*-
#import gevent
from time import mktime
from iso8601 import parse_date
from socket import setdefaulttimeout
from retrying import retry

from openprocurement_client.client import TendersClient
from openprocurement.search.source import BaseSource

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
        'tender_preload': 500000,
        'concurrency': 5,
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['tender_limit'] = int(self.config['tender_limit'] or 0) or 100
        self.config['tender_preload'] = int(self.config['tender_preload'] or 0) or 100
        self.config['concurrency'] = int(self.config['concurrency'] or 0)
        self.client = None

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
        if 'awards' in tender['data']:
            for award in tender['data']['awards']:
                if award.get('status') == 'active':
                    award['activeDate'] = award.get('date')
        if 'contracts' in tender['data']:
            for contract in tender['data']['contracts']:
                if contract.get('status') == 'active':
                    contract['activeDate'] = contract.get('date')
        return tender

    @retry(stop_max_attempt_number=5, wait_fixed=15000)
    def reset(self):
        logger.info("Reset tenders, tender_skip_until=%s", self.config['tender_skip_until'])
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
            params=params)
        self.skip_until = self.config.get('tender_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None

    def preload(self):
        preload_items = []
        while True:
            try:
                items = self.client.get_tenders()
            except Exception as e:
                logger.error("TenderSource.preload error %s", str(e))
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
                    str(item['id']), retry_count, str(e))
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

    def get_all(self, items):
        if len(items) < 10 or self.config['concurrency'] < 2:
            for i in items:
                yield self.get(i)
            return

        completed = 0
        while items:
            jobs = []
            while items and len(jobs) < self.config['concurrency']:
                jobs.append(gevent.spawn(self.get, items.pop()))
            for res in gevent.joinall(jobs):
                completed += 1
                yield res.value
            # logger.info("%s.GetAll %d jobs completed %d left",
            #     self.__class__.__name__, completed, len(items))
