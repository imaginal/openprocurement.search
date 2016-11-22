# -*- coding: utf-8 -*-
from time import sleep
from logging import getLogger
logger = getLogger(__name__)

class BaseSource:
    """Data Source Interface
    """
    should_exit = False

    @property
    def doc_type(self):
        return self.__doc_type__

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
