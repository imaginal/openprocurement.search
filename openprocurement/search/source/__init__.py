# -*- coding: utf-8 -*-

class BaseSource:
    """Data Source Interface
    """
    def reset(self):
        pass

    def items(self, name=None):
        return []

    def get(self, item):
        return item
