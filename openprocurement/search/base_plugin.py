# -*- coding: utf-8 -*-


class BasePlugin(object):
    __name__ = __name__
    index_mappings = {}
    search_maps = {}

    # Plugin api
    start_in_subprocess = None
    before_create_index = None
    before_source_reset = None
    before_source_items = None
    before_process_index = None
    before_index_item = None

    def __init__(self, config):
        pass

    def __repr__(self):
        return "<%s:%s>" % (self.__name__, self.__class__.__name__)
