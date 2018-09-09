# -*- coding: utf-8 -*-

PLUGIN_API_VERSION = 2.0


class BasePlugin(object):
    __name__ = __name__
    plugin_api_version = 0
    index_mappings = {}
    search_maps = {}

    # Plugin api
    before_fork_process = None
    before_create_index = None
    before_source_reset = None
    before_source_items = None
    before_index_source = None
    before_index_item = None

    def __init__(self, config):
        pass

    def __repr__(self):
        return "<%s:%s>" % (self.__name__, self.__class__.__name__)
