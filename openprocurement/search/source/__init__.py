# -*- coding: utf-8 -*-
import os
import os.path
import gzip
import simplejson as json
from time import sleep
from munch import munchify
from socket import setdefaulttimeout
from openprocurement.search.version import __version__
from openprocurement_client import client

from logging import getLogger
logger = getLogger(__name__)


class BaseSource:
    """Data Source Interface
    """
    should_exit = False
    should_reset = False
    last_reset_time = 0
    client_user_agent = 'Search-Tenders/%s' % __version__
    cache_path = None
    cache_hits = 0
    cache_miss = 0
    cache_puts = 0
    stat_resets = 0
    stat_queries = 0
    stat_fetched = 0
    stat_skipped = 0
    stat_getitem = 0
    preload_wait = 0.1

    @property
    def doc_type(self):
        return self.__doc_type__

    def need_reset(self):
        return False

    def reset(self):
        pass

    def items(self, name=None):
        return []

    def get(self, item):
        self.stat_getitem += 1
        return item

    def get_all(self, items):
        out = [self.get(i) for i in items]
        return out

    def sleep(self, seconds):
        if not isinstance(seconds, float):
            seconds = float(seconds)
        while not self.should_exit and seconds > 0:
            sleep(0.1 if seconds > 0.1 else seconds)
            seconds -= 0.1

    def cache_setpath(self, base, host, version, resource):
        if not base or len(base) < 4:
            return
        if '://' in host:
            host = host.replace('://', '_')
        self.cache_path = os.path.join(base, host, version, resource)
        logger.info("Enable %s cahce %s", resource, self.cache_path)

    def cache_dirname(self, name):
        if len(name) < 4:
            raise ValueError("Bad name %s" % name)
        return os.path.join(self.cache_path, name[:2], name[2:4])

    def cahce_filename(self, name, dirname=None):
        dirname = self.cache_dirname(name)
        return os.path.join(dirname, name + '.gz')

    def cache_allow(self, data):
        return data and len(data['data']) > 5

    def cache_get(self, item):
        cache_total = self.cache_hits + self.cache_miss
        if cache_total > 0 and cache_total % 10000 == 0:
            cache_usage = 100 * self.cache_hits / cache_total
            logger.info("[%s] File cache %d hits, %d miss, %d puts, usage %d%%",
                        self.__doc_type__, self.cache_hits, self.cache_miss,
                        self.cache_puts, cache_usage)

        filename = self.cahce_filename(item['id'])
        if not os.path.exists(filename):
            self.cache_miss += 1
            return {}
        try:
            with gzip.open(filename, 'rb') as fp:
                data = json.load(fp, encoding='utf-8')
            if data['data']['dateModified'] == item['dateModified']:
                assert data['data']['id'] == item['id'], "Bad ID"
                assert len(data['data']) > 5, "Bad data"
                if 'meta' in data:
                    data['meta']['from_cache'] = True
                if self.cache_allow(data):
                    self.cache_hits += 1
                    return munchify(data)
            os.remove(filename)
        except Exception as e:
            logger.error("Can't get from cache %s error: %s", filename, str(e))
        self.cache_miss += 1
        return {}

    def cache_put(self, data):
        if not self.cache_path:
            return
        try:
            if not self.cache_allow(data):
                return data
            dirname = self.cache_dirname(data['data']['id'])
            filename = self.cahce_filename(data['data']['id'])
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            elif os.path.exists(filename):
                os.remove(filename)
            data = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            if not isinstance(data, str) and isinstance(data, unicode):
                data = data.encode('utf-8')
            with gzip.open(filename, 'wb') as fp:
                fp.write(data)
            self.cache_puts += 1
        except Exception as e:
            logger.error("Can't save to cache %s error: %s", str(data), str(e))
        return data

    def disable_cache(self):
        self.cache_path = None


class TendersClient(client.TendersClient):
    def __init__(self, *args, **kwargs):
        self.user_agent = kwargs.pop('user_agent', None)
        self.timeout = kwargs.pop('timeout', 300)
        if self.timeout:
            setdefaulttimeout(self.timeout)
        super(TendersClient, self).__init__(*args, **kwargs)

    def request(self, *args, **kwargs):
        if 'User-Agent' not in self.headers and self.user_agent:
            self.headers['User-Agent'] = self.user_agent
        return super(TendersClient, self).request(*args, **kwargs)
