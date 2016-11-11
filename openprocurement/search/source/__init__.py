# -*- coding: utf-8 -*-

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
