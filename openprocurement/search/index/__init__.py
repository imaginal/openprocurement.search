# -*- coding: utf-8 -*-
from time import time, sleep
from logging import getLogger

logger = getLogger(__name__)


class BaseIndex:
    """Search Index Interface
    """
    config = {
        'index_speed': 20,
    }
    def __init__(self, engine, source, config={}):
        assert(self.__index_name__)
        if config:
            self.config.update(config)
            self.config['index_speed'] = float(self.config['index_speed'])
        self.source = source
        self.engine = engine
        self.engine.add_index(self)

    def __repr__(self):
        return self.__index_name__

    @property
    def current_index(self):
        key = self.__index_name__
        return self.engine.get_index(key)

    def index_age(self, name=None):
        if not name:
            name = self.current_index
        if not name:
            return time()
        prefix, suffix = name.split('_')
        return int(time() - int(suffix))

    def need_reindex(self):
        return not self.current_index

    def create_index(self, name):
        return

    def finish_index(self, name):
        return

    def new_index(self):
        index_key = self.__index_name__
        index_new = index_key + '.new'
        # try restore last index (in case of crash)
        name = self.engine.get_index(index_new)
        current_index = self.current_index
        if current_index and name == current_index:
            name = None
        if not name:
            name = "{}_{}".format(index_key, int(time()))
            self.create_index(name)
            self.engine.set_index(index_new, name)
        # also set current if empty
        current = self.engine.get_index(index_key)
        if not current:
            self.engine.set_index(index_key, name)
        return name

    def delete_index(self, name):
        # TODO: real delete index
        return

    def set_current(self, name):
        logger.warning("Set current, index %s", name)
        old_index = self.current_index
        index_key, suffix = name.split('_')
        self.engine.set_index(index_key, name)
        assert(self.current_index == name)
        # remove .new index key
        index_new = index_key + '.new'
        if self.engine.get_index(index_new) == name:
            self.engine.set_index(index_new, '')
        self.delete_index(old_index)

    def test_exists(self, index_name, info):
        return self.engine.test_exists(index_name, info)

    def test_noindex(self, item):
        return False

    def index_item(self, index_name, item):
        if self.test_noindex(item):
            logger.warning("Noindex %s", item.data.id)
            return None
        return self.engine.index_item(index_name, item)

    def index_source(self, index_name=None, reset=False):
        if not index_name:
            index_name = self.current_index
        assert(index_name)

        source = self.source
        if reset:
            source.reset()

        count = 0
        while True:
            items_list = source.items()
            iter_count = 0
            for info in items_list:
                if not self.test_exists(index_name, info):
                    item = source.get(info)
                    if self.index_item(index_name, item):
                        count += 1
                    if count % 100 == 0:
                        sleep(1)
                iter_count += 1
            # break on empty set
            if not iter_count:
                break
            pause = iter_count / float(self.config['index_speed'])
            logger.info("Fetched %d indexed %d last %s wait %1.1fs",
                iter_count, count, info.get('dateModified'), pause)
            sleep(pause)

        return count

    def process(self):
        if self.need_reindex():
            index_name = self.new_index()
            logger.warning("Starting full re-index, new index %s",
                index_name)
            self.index_source(index_name, reset=True)
            self.finish_index(index_name)
            self.set_current(index_name)

        return self.index_source()
