# -*- coding: utf-8 -*-
from time import mktime
from iso8601 import parse_date

from openprocurement_client.client import Client
from openprocurement.search.source import BaseSource


class TenderSource(BaseSource):
    """Tenders Source from open openprocurement.API
    """
    __doc_type__ = 'tender'

    config = {
        'api_key': '',
        'api_url': "https://api-sandbox.openprocurement.org",
        'api_version': '0.8'
    }
    def __init__(self, config={}):
        if config:
            self.config.update(config)
        client = Client(key=self.config['api_key'],
            host_url=self.config['api_url'],
            api_version=self.config['api_version'])
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
        tender_list = self.client.get_tenders()
        for tender in tender_list:
            yield self.patch_version(tender)

    def get(self, item):
        tender = self.client.get_tender(item['id'])
        tender['meta'] = item
        return tender
