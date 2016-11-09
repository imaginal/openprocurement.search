# -*- coding: utf-8 -*-
from time import mktime, sleep
from iso8601 import parse_date
from socket import setdefaulttimeout
from retrying import retry

from openprocurement_client.client import TendersClient
from openprocurement.search.source import BaseSource

from logging import getLogger
logger = getLogger(__name__)


class PlanSource(BaseSource):
    """Tenders Source from open openprocurement.API.plans
    """
    __doc_type__ = 'plan'

    config = {
        'plan_api_key': '',
        'plan_api_url': "",
        'plan_api_version': '0',
        'plan_resource': 'plans',
        'plan_api_mode': '',
        'plan_skip_until': None,
        'plan_limit': 1000,
        'plan_preload': 500000,
        'timeout': 30,
    }

    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.config['plan_limit'] = int(self.config['plan_limit']) or 100
        self.config['plan_preload'] = int(self.config['plan_preload']) or 100
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

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset plans, plan_skip_until=%s",
                    self.config['plan_skip_until'])
        if self.config.get('timeout', None):
            setdefaulttimeout(float(self.config['timeout']))
        params = {}
        if self.config['plan_api_mode']:
            params['mode'] = self.config['plan_api_mode']
        if self.config['plan_limit']:
            params['limit'] = self.config['plan_limit']
        self.client = TendersClient(
            key=self.config['plan_api_key'],
            host_url=self.config['plan_api_url'],
            api_version=self.config['plan_api_version'],
            resource=self.config['plan_resource'],
            params=params)
        self.skip_until = self.config.get('plan_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None

    def preload(self):
        preload_items = []
        items = True
        while items:
            items = self.client.get_tenders()
            if items:
                preload_items.extend(items)
                logger.info("Preload %d plans, last %s",
                            len(preload_items), items[-1]['dateModified'])
            if items and len(items) < 10:
                break
            if len(preload_items) >= self.config['plan_preload']:
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
        while not tender:
            try:
                tender = self.client.get_tender(item['id'])
                break
            except Exception as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("PlanSource.get_plan %s error %s %s",
                    str(item), str(e.__class__), str(e))
                sleep(float(self.config['timeout']))
                if retry_count > 2:
                    logger.warning("PlanSource.reset after error")
                    self.reset()
        tender['meta'] = item
        return tender
