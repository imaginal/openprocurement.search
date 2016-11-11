# -*- coding: utf-8 -*-
from time import mktime, sleep
from iso8601 import parse_date
from socket import setdefaulttimeout
from retrying import retry
from restkit import ResourceError

from openprocurement_client.client import TendersClient
from openprocurement.search.source import BaseSource

from logging import getLogger
logger = getLogger(__name__)


class TenderSource(BaseSource):
    """Tenders Source from open openprocurement.API
    """
    __doc_type__ = 'tender'

    config = {
        'api_key': '',
        'api_url': "",
        'api_version': '0',
        'api_mode': '',
        'skip_until': None,
        'limit': 1000,
        'preload': 500000,
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['limit'] = int(self.config['limit']) or 100
        self.config['preload'] = int(self.config['preload']) or 100
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

    @retry(stop_max_attempt_number=5, wait_fixed=15000)
    def reset(self):
        logger.info("Reset tenders, skip_until=%s", self.config['skip_until'])
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        params = {}
        if self.config['api_mode']:
            params['mode'] = self.config['api_mode']
        if self.config['limit']:
            params['limit'] = self.config['limit']
        self.client = TendersClient(
            key=self.config['api_key'],
            host_url=self.config['api_url'],
            api_version=self.config['api_version'],
            params=params)
        self.skip_until = self.config.get('skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None

    def preload(self):
        preload_items = []
        while True:
            if self.should_exit:
                return []
            try:
                items = self.client.get_tenders()
            except ResourceError as e:
                logger.error("TenderSource.preload error %s", str(e))
                self.reset()
                break
            if not items:
                break

            preload_items.extend(items)

            if len(preload_items) >= 100:
                logger.info(
                    "Preload %d tenders, last %s",
                    len(preload_items),
                    items[-1]['dateModified'])
            if len(items) < 10:
                break
            if len(preload_items) >= self.config['preload']:
                break

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for tender in self.preload():
            if self.skip_until > tender['dateModified']:
                self.last_skipped = tender['dateModified']
                continue
            yield self.patch_version(tender)

    def get(self, item):
        tender = None
        retry_count = 0
        while not self.should_exit:
            try:
                tender = self.client.get_tender(item['id'])
                break
            except ResourceError as e:
                if retry_count > 3:
                    raise e
                logger.error("TenderSource.get_tender %s error %s",
                    str(item['id']), str(e))
                if retry_count > 1:
                    self.reset()
                retry_count += 1
                sleep(1)
        tender['meta'] = item
        return tender
