# -*- coding: utf-8 -*-

from logging import getLogger
logger = getLogger(__name__)

class BaseSource:
    """Data Source Interface
    """
    def reset(self):
        pass

    def items(self, name=None):
        return []

    def get(self, item):
        return item
