# -*- coding: utf-8 -*-
from time import mktime
from retrying import retry
from iso8601 import parse_date
from socket import setdefaulttimeout

from openprocurement_client.client import Client
from openprocurement.search.source import BaseSource, logger


class PlanSource(BaseSource):
    """Tenders Source from open openprocurement.API.plans
    """
    __doc_type__ = 'plan'

    config = {
        'plan_api_key': '',
        'plan_api_url': "",
        'plan_api_version': '0',
        'plan_resource': 'plans',
        'plan_params': {},
        'plan_skip_until': None,
        'timeout': 30,
    }
    def __init__(self, config={}):
        if config:
            self.config.update(config)
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

    def reset(self):
        logger.info("Reset plans, plan_skip_until %s", self.config['plan_skip_until'])
        self.client = Client(key=self.config['plan_api_key'],
            host_url=self.config['plan_api_url'],
            api_version=self.config['plan_api_version'],
            resource=self.config['plan_resource'],
            timeout=self.config['timeout'],
            params=self.config['plan_params'])

    def items(self):
        if not self.client:
            self.reset()
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        skip_until = self.config.get('plan_skip_until', None)
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
