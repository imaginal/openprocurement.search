# -*- coding: utf-8 -*-
import os
import os.path
import gzip
import simplejson as json
from time import sleep
from munch import munchify
from socket import setdefaulttimeout
from openprocurement.search import __version__
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
    cache_hit = 0
    cache_miss = 0

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

    def cache_get(self, item):
        filename = self.cahce_filename(item['id'])
        if not os.path.exists(filename):
            self.cache_miss += 1
            return {}
        if self.cache_hit + self.cache_miss % 10000 == 0:
            logger.info("[%s] Cache usage %d / %d", self.__doc_type__,
                self.cache_hit, self.cache_miss)
        try:
            with gzip.open(filename, 'rb') as fp:
                data = json.load(fp)
            if data['data']['dateModified'] == item['dateModified']:
                if len(data['data'].keys()) > 4:
                    self.cache_hit += 1
                    return munchify(data)
            os.remove(filename)
        except Exception as e:
            logger.error("Can't get from cache %s %s", filename, str(e))
        self.cache_miss += 1
        return {}

    def cache_put(self, item):
        try:
            dirname = self.cache_dirname(item['data']['id'])
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            filename = self.cahce_filename(item['data']['id'])
            with gzip.open(filename, 'wb') as fp:
                json.dump(item, fp)
        except Exception as e:
            filename = item.get('data', {}).get('id')
            logger.error("Can't save to cache %s %s", filename, str(e))
            self.cache_path = None
        return item


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
