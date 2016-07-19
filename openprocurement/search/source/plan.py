# -*- coding: utf-8 -*-
from time import mktime
from retrying import retry
from iso8601 import parse_date
from socket import setdefaulttimeout

from openprocurement_client.client import Client
from openprocurement.search.source import BaseSource


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
        'timeout': 30,
    }
    def __init__(self, config={}):
        if config:
            self.config.update(config)
        client = Client(key=self.config['plan_api_key'],
            host_url=self.config['plan_api_url'],
            api_version=self.config['plan_api_version'],
            resource=self.config['plan_resource'],
            timeout=self.config['timeout'],
            params=self.config['plan_params'])
        self.client = client

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        dt = parse_date(item['dateModified'])
        version = 1e6 * mktime(dt.timetuple()) + dt.microsecond
        item['version'] = long(version)
        return item

    def reset(self):
        self.client.params.pop('offset', None)

    def items(self):
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        tender_list = self.client.get_tenders()
        for tender in tender_list:
            yield self.patch_version(tender)

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def get(self, item):
        tender = self.client.get_tender(item['id'])
        tender['meta'] = item
        return tender
