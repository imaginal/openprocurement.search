# -*- coding: utf-8 -*-
from time import sleep
from logging import getLogger
from ..version import __version__

from openprocurement_client import client

logger = getLogger(__name__)

class BaseSource:
    """Data Source Interface
    """
    should_exit = False
    should_reset = False
    last_reset_time = 0
    client_user_agent = 'Search-Tenders/%s' % __version__

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



class TendersClient(client.TendersClient):
    def __init__(self, *args, **kwargs):
        self.user_agent = kwargs.pop('user_agent', None)
        self.timeout = kwargs.pop('timeout', 300)
        if self.timeout: setdefaulttimeout(self.timeout)
        super(MyTendersClient, self).__init__(*args, **kwargs)

    def request(self, *args, **kwargs):
        if 'User-Agent' not in self.headers and self.user_agent:
            self.headers['User-Agent'] = self.user_agent
        return super(MyTendersClient, self).request(*args, **kwargs)
