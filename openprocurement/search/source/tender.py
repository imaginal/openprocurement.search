# -*- coding: utf-8 -*-
from time import mktime
from retrying import retry
from iso8601 import parse_date
from socket import setdefaulttimeout
from logging import getLogger

from openprocurement_client.client import Client
from openprocurement.search.source import BaseSource

logger = getLogger(__name__)


class TenderSource(BaseSource):
    """Tenders Source from open openprocurement.API
    """
    __doc_type__ = 'tender'

    config = {
        'api_key': '',
        'api_url': "",
        'api_version': '0',
        'params': {},
        'skip_until': None,
        'timeout': 30,
    }
    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.client = Client(key=self.config['api_key'],
            host_url=self.config['api_url'],
            api_version=self.config['api_version'],
            timeout=self.config['timeout'],
            params=self.config['params'])

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        dt = parse_date(item['dateModified'])
        version = 1e6 * mktime(dt.timetuple()) + dt.microsecond
        item['version'] = long(version)
        return item

    def reset(self):
        logger.info("Reset tenders, skip_until %s", self.config['skip_until'])
        self.client.params.pop('offset', None)

    def items(self):
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        skip_until = self.config.get('skip_until', None)
        if skip_until and skip_until[:2] != '20':
            skip_until = None
        tender_list = self.client.get_tenders()
        for tender in tender_list:
            if skip_until and skip_until > tender['dateModified']:
                continue
            yield self.patch_version(tender)

    @retry(stop_max_attempt_number=5, wait_fixed=15000)
    def get(self, item):
        tender = self.client.get_tender(item['id'])
        tender['meta'] = item
        return tender
