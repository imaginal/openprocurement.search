# -*- coding: utf-8 -*-


class BasePlugin(object):
    __name__ = __name__
    search_maps = {}

    def __init__(self, config):
        pass

    def __repr__(self):
        return "<%s:%s>" % (self.__name__, self.__class__.__name__)

    def before_index_item(self, index, item):
        pass
