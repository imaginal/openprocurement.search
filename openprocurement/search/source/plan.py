# -*- coding: utf-8 -*-
from time import time, mktime
from datetime import datetime, timedelta
from iso8601 import parse_date
from socket import setdefaulttimeout
from retrying import retry

from openprocurement.search.source import BaseSource, TendersClient
from openprocurement.search.source.orgs import OrgsDecoder
from openprocurement.search.utils import restkit_error

from logging import getLogger
logger = getLogger(__name__)


class PlanSource(BaseSource):
    """Plan Source from open openprocurement.API.plans
    """
    __doc_type__ = 'plan'

    config = {
        'plan_api_key': '',
        'plan_api_url': "",
        'plan_api_version': '0',
        'plan_resource': 'plans',
        'plan_api_mode': '',
        'plan_skip_after': None,
        'plan_skip_until': None,
        'plan_limit': 1000,
        'plan_preload': 10000,
        'plan_reseteach': 3,
        'plan_resethour': 23,
        'plan_decode_orgs': False,
        'plan_fast_client': False,
        'plan_fast_stepsback': 10,
        'plan_user_agent': '',
        'plan_file_cache': '',
        'plan_cache_minage': 15,
        'timeout': 30,
    }

    def __init__(self, config={}, use_cache=False):
        if config:
            self.config.update(config)
        self.config['plan_limit'] = int(self.config['plan_limit'] or 0) or 100
        self.config['plan_preload'] = int(self.config['plan_preload'] or 0) or 100
        self.config['plan_reseteach'] = int(self.config['plan_reseteach'] or 3)
        self.config['plan_resethour'] = int(self.config['plan_resethour'] or 0)
        self.client_user_agent += " (plans) " + self.config['plan_user_agent']
        if use_cache:
            self.cache_setpath(self.config['plan_file_cache'], self.config['plan_api_url'],
                self.config['plan_api_version'], 'plans')
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

    def patch_plan(self, plan):
        if 'date' not in plan['data']:
            if 'datePublished' in plan['data']:
                plan['data']['date'] = plan['data']['datePublished']
            else:
                planID = plan['data']['planID']
                pos = planID.find('-20')
                plan['data']['date'] = planID[pos+1:pos+11]
        # decode official org name from EDRPOU registry
        if self.config['plan_decode_orgs'] and self.orgs_db:
            if 'procuringEntity' in plan['data']:
                self.orgs_db.patch_entity(plan['data']['procuringEntity'])
        return plan

    def need_reset(self):
        if self.should_reset:
            return True
        if time() - self.last_reset_time > 3600 * int(self.config['plan_reseteach']):
            return True
        if time() - self.last_reset_time > 3600:
            return datetime.now().hour == int(self.config['plan_resethour'])

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset(self):
        logger.info("Reset plans, plan_skip_until=%s plan_skip_after=%s",
                    self.config['plan_skip_until'], self.config['plan_skip_after'])
        self.stat_resets += 1
        if self.config['plan_decode_orgs']:
            self.orgs_db = OrgsDecoder(self.config)
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
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        logger.info("PlansClient %s", self.client.headers)
        if self.config['plan_fast_client']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['plan_api_key'],
                host_url=self.config['plan_api_url'],
                api_version=self.config['plan_api_version'],
                resource=self.config['plan_resource'],
                params=fast_params,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent+" fast_client")
            for i in range(int(self.config['plan_fast_stepsback'])):
                self.fast_client.get_tenders()
            self.fast_client.params.pop('descending')
            logger.info("PlansClient (fast) %s", self.fast_client.headers)
        else:
            self.fast_client = None
        if self.config['plan_file_cache'] and self.cache_path:
            cache_minage = int(self.config['plan_cache_minage'])
            cache_date = datetime.now() - timedelta(days=cache_minage)
            self.cache_allow_dateModified = cache_date.isoformat()
            logger.info("[plan] Cache allow dateModified before %s",
                        self.cache_allow_dateModified)
        self.skip_until = self.config.get('plan_skip_until', None)
        if self.skip_until and self.skip_until[:2] != '20':
            self.skip_until = None
        self.skip_after = self.config.get('plan_skip_after', None)
        if self.skip_after and self.skip_after[:2] != '20':
            self.skip_after = None
        self.last_reset_time = time()
        self.should_reset = False

    def preload(self):
        preload_items = []
        # try prelaod last plans first
        if self.fast_client:
            try:
                items = self.fast_client.get_tenders()
                self.stat_queries += 1
                if not len(items):
                    logger.debug("Preload fast 0 plans")
                    raise ValueError()
                preload_items.extend(items)
                logger.info("Preload fast %d plans, last %s",
                    len(preload_items), items[-1]['dateModified'])
            except:
                pass

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
                    retry_count, len(preload_items), restkit_error(e, self.client))
                self.sleep(5 * retry_count)
                self.reset()
                continue
            if not items:
                break

            preload_items.extend(items)

            if len(preload_items) >= 100:
                logger.info("Preload %d plans, last %s",
                    len(preload_items), items[-1]['dateModified'])
            if len(items) < 10:
                self.fast_client = None
                break
            if len(preload_items) >= self.config['plan_preload']:
                break

        return preload_items

    def items(self):
        if not self.client:
            self.reset()
        self.last_skipped = None
        for plan in self.preload():
            if self.should_exit:
                raise StopIteration()
            if self.skip_until and self.skip_until > plan['dateModified']:
                self.last_skipped = plan['dateModified']
                self.stat_skipped += 1
                continue
            if self.skip_after and self.skip_after < plan['dateModified']:
                self.last_skipped = plan['dateModified']
                self.stat_skipped += 1
                continue
            self.stat_fetched += 1
            yield self.patch_version(plan)

    def cache_allow(self, data):
        if data and data['data']['dateModified'] < self.cache_allow_dateModified:
            return True
        return False

    def get(self, item):
        plan = {}
        retry_count = 0
        if self.cache_path:
            plan = self.cache_get(item)
        while not plan:
            if self.should_exit:
                break
            try:
                plan = self.client.get_tender(item['id'])
                assert plan['data']['id'] == item['id'], "plan.id"
                assert plan['data']['dateModified'] >= item['dateModified'], "plan.dateModified"
            except Exception as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("GET %s/%s retry %d error %s", self.client.prefix_path,
                    str(item['id']), retry_count, restkit_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
                    self.reset()
                plan = {}
            # save to cache
            if plan and self.cache_path:
                self.cache_put(plan)

        if item['dateModified'] != plan['data']['dateModified']:
            logger.debug("Plan dateModified mismatch %s %s %s",
                item['id'], item['dateModified'],
                plan['data']['dateModified'])
            item['dateModified'] = plan['data']['dateModified']
            item = self.patch_version(item)
        plan['meta'] = item
        self.stat_getitem += 1
        return self.patch_plan(plan)
