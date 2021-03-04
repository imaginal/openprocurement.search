# -*- coding: utf-8 -*-
from time import time
from random import random
from datetime import datetime, timedelta

from openprocurement.search.source import BaseSource, TendersClient
from openprocurement.search.source.orgs import OrgsDecoder
from openprocurement.search.utils import long_version, request_error, retry

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
        'plan_preload': 5000,
        'plan_reseteach': 13,
        'plan_resethour': 0,
        'plan_decode_orgs': False,
        'plan_fast_client': 0,
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
        self.config['plan_reseteach'] = float(self.config['plan_reseteach'] or 0)
        self.config['plan_resethour'] = int(self.config['plan_resethour'] or 0)
        if self.config['plan_reseteach'] > 1:
            self.config['plan_reseteach'] += random()
        self.client_user_agent += " (plans) " + self.config['plan_user_agent']
        if use_cache:
            self.cache_setpath(self.config['plan_file_cache'], self.config['plan_api_url'],
                self.config['plan_api_version'], 'plans')
        self.preload_wait = 1.0 / float(config.get('query_speed', 100))
        self.fast_client = None
        self.client = None
        self.orgs_db = None

    def procuring_entity(self, item):
        return item.data.get('procuringEntity', None)

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        item['version'] = long_version(item['dateModified'])
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
        if self.last_preload_count >= 50 or time() - self.last_reset_time < 3600:
            return False
        if self.config['plan_reseteach'] and (time() - self.last_reset_time > 3600 * self.config['plan_reseteach']):
            logger.info("Reset by plan_reseteach=%s", str(self.config['plan_reseteach']))
            return True
        if self.config['plan_resethour'] and (datetime.now().hour == int(self.config['plan_resethour'])):
            logger.info("Reset by plan_resethour=%s", str(self.config['plan_resethour']))
            return True

    @retry(5, logger=logger)
    def reset(self):
        logger.info("Reset plans client, plan_skip_until=%s plan_skip_after=%s plan_fast_client=%s",
                    self.config['plan_skip_until'], self.config['plan_skip_after'], self.config['plan_fast_client'])
        self.stat_resets += 1
        if self.config['plan_decode_orgs']:
            self.orgs_db = OrgsDecoder(self.config)
        params = {}
        if self.config['plan_api_mode']:
            params['mode'] = self.config['plan_api_mode']
        if self.config['plan_limit']:
            params['limit'] = self.config['plan_limit']
        if self.client:
            self.client.close()
        self.client = TendersClient(
            key=self.config['plan_api_key'],
            host_url=self.config['plan_api_url'],
            api_version=self.config['plan_api_version'],
            resource=self.config['plan_resource'],
            params=params,
            timeout=float(self.config['timeout']),
            user_agent=self.client_user_agent)
        logger.info("PlansClient params %s/api/%s %s",
            self.config['plan_api_url'], self.config['plan_api_version'], self.client.params)
        logger.info("PlansClient cookie %s", self.client.cookies)
        if self.fast_client:
            self.fast_client.close()
            self.fast_client = None
        if str(self.config['plan_fast_client']).strip() == "2":
            # main client from present to future
            self.client.params['descending'] = 1
            self.client.get_tenders()
            self.client.params.pop('descending')
            # self.client.get_tenders()
            # fast client from present to past
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['plan_api_key'],
                host_url=self.config['plan_api_url'],
                api_version=self.config['plan_api_version'],
                resource=self.config['plan_resource'],
                params=fast_params,
                session=self.client.session,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + " back_client")
            logger.info("PlansClient (back) params %s/api/%s %s",
                self.config['plan_api_url'], self.config['plan_api_version'], str(self.fast_client.params))
            logger.info("PlansClient (back) cookie %s", self.fast_client.cookies)
        elif self.config['plan_fast_client']:
            fast_params = dict(params)
            fast_params['descending'] = 1
            self.fast_client = TendersClient(
                key=self.config['plan_api_key'],
                host_url=self.config['plan_api_url'],
                api_version=self.config['plan_api_version'],
                resource=self.config['plan_resource'],
                params=fast_params,
                session=self.client.session,
                timeout=float(self.config['timeout']),
                user_agent=self.client_user_agent + " fast_client")
            for i in range(int(self.config['plan_fast_stepsback'])):
                self.fast_client.get_tenders()
                self.sleep(self.preload_wait)
            self.fast_client.params.pop('descending')
            logger.info("PlansClient (back) params %s/api/%s %s",
                self.config['plan_api_url'], self.config['plan_api_version'], str(self.fast_client.params))
            logger.info("PlansClient (fast) cookie %s", self.fast_client.cookies)
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
        self.last_preload_count = 0
        self.should_reset = False

    def preload(self):
        preload_items = []
        items = None

        # try prelaod last plans first
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

            if len(items) < 10:
                break
            if len(preload_items) >= self.config['plan_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

        if items and len(items) >= 10 and 'dateModified' in items[-1]:
            logger.info("Preload %d plans, last %s", len(preload_items), items[-1]['dateModified'])

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

            if len(items) < 10:
                break
            if len(preload_items) >= self.config['plan_preload']:
                break
            if self.preload_wait:
                self.sleep(self.preload_wait)

        if items and len(items) >= 10 and 'dateModified' in items[-1]:
            logger.info("Preload %d plans, last %s", len(preload_items), items[-1]['dateModified'])

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
                self.last_yielded = plan['dateModified']
                self.stat_fetched += 1
                yield self.patch_version(plan)

            if self.last_skipped:
                logger.info("Skipped %d plans, last %s", self.stat_skipped, self.last_skipped)
            if self.last_yielded or not self.last_skipped:
                break

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
                # except dates with zero microsec like 2021-02-26T14:30:00+02:00 < 2021-02-26T14:30:00.000000+02:00
                if plan['data']['dateModified'] < item['dateModified'] and ".000000" not in item['dateModified']:
                    assert plan['data']['dateModified'] >= item['dateModified'], "plan.dateModified"
            except Exception as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("GET %s/%s meta %s retry %d error %s", self.client.prefix_path,
                    str(item['id']), str(item), retry_count, request_error(e, self.client))
                self.sleep(5 * retry_count)
                if retry_count > 1:
                    self.reset()
                plan = {}
            # save to cache
            if plan and self.cache_path:
                self.cache_put(plan)

        if item['dateModified'] != plan['data']['dateModified']:
            if not item.pop('ignore_dateModified', False):
                logger.debug("[plan] dateModified mismatch %s %s %s",
                    item['id'], item['dateModified'],
                    plan['data']['dateModified'])
            item['dateModified'] = plan['data']['dateModified']
            item = self.patch_version(item)
        plan['meta'] = item
        self.stat_getitem += 1
        return self.patch_plan(plan)
