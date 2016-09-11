# -*- coding: utf-8 -*-
from time import time, sleep
from logging import getLogger

logger = getLogger(__name__)


class BaseIndex:
    """Search Index Interface
    """
    config = {
        'index_speed': 100.0,
    }
    def __init__(self, engine, source, config={}):
        assert(self.__index_name__)
        if config:
            self.config.update(config)
            self.config['index_speed'] = float(self.config['index_speed'])
        rename_index = 'rename_' + self.__index_name__
        if rename_index in self.config:
            self.__index_name__ = self.config[rename_index]
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
        prefix, suffix = name.rsplit('_', 1)
        return int(time() - int(suffix))

    def need_reindex(self):
        return not self.current_index

    def create_index(self, name):
        return

    def finish_index(self, name):
        return

    def new_index(self):
        index_key = self.__index_name__
        index_key_next = "{}.next".format(index_key)
        # try restore last index (in case of crash)
        name = self.engine.get_index(index_key_next)
        current_index = self.current_index
        if current_index and name == current_index:
            name = None
        if self.index_age(name) > 86400:
            name = None
        if not name:
            name = "{}_{}".format(index_key, int(time()))
            self.create_index(name)
            self.engine.set_index(index_key_next, name)
        # also set current if empty
        current = self.engine.get_index(index_key)
        if not current:
            self.engine.set_index(index_key, name)
        return name

    def delete_index(self, name):
        index_key = self.__index_name__
        index_key_prev = "{}.prev".format(index_key)
        self.engine.set_index(index_key_prev, name)

    def set_current(self, name):
        index_key = self.__index_name__
        old_index = self.current_index
        if name != old_index:
            logger.info("Change current index %s -> %s",
                old_index, name)
            self.engine.set_index(index_key, name)
            assert(self.current_index == name)
            self.delete_index(old_index)
        # remove index.next key
        index_key_next = "{}.next".format(index_key)
        if self.engine.get_index(index_key_next) == name:
            self.engine.set_index(index_key_next, '')

    def test_exists(self, index_name, info):
        return self.engine.test_exists(index_name, info)

    def test_noindex(self, item):
        return False

    def before_index_item(self, item):
        return

    def indexing_stat(self, index_name, fetched, indexed, iter_count, last_date):
        last_date = last_date or ""
        pause = 1.0 * iter_count / self.config['index_speed']
        logger.info("[%s] Fetched %d indexed %d last %s",
            index_name, fetched, indexed, last_date[:19])
        sleep(pause)

    def index_item(self, index_name, item):
        if self.test_noindex(item):
            logger.debug("[%s] Noindex %s %s", index_name,
                item.data.id, item.data.get('tenderID', ''))
            return None
        self.before_index_item(item)
        return self.engine.index_item(index_name, item)

    def index_source(self, index_name=None, reset=False):
        if reset:
            self.source.reset()

        if not index_name:
            index_name = self.current_index

        if not index_name and self.engine.slave_mode:
            self.engine.heartbeat(self.source)
            index_name = self.current_index

        if not index_name:
            logger.warning("No current index for %s", repr(self))
            return

        index_count = 0
        total_count = 0
        # heartbeat return False in slave mode if master is ok
        # heartbeat always True in master mode
        while self.engine.heartbeat(self.source):
            info = None
            iter_count = 0
            for info in self.source.items():
                if not self.test_exists(index_name, info):
                    item = self.source.get(info)
                    if self.index_item(index_name, item):
                        index_count += 1
                iter_count += 1
                total_count += 1
                # update heartbeat for long indexing
                if iter_count >= 500:
                    self.indexing_stat(index_name, total_count, index_count,
                        iter_count, info.get('dateModified'))
                    self.engine.heartbeat(self.source)
                    iter_count = 0

            # break if nothing iterated
            if iter_count:
                self.indexing_stat(index_name, total_count, index_count,
                    iter_count, info.get('dateModified'))
            else:
                break

        return index_count

    def process(self, allow_reindex = True):
        if self.need_reindex() and allow_reindex:
            index_name = self.new_index()
            logger.info("Starting full reindex, new index %s", index_name)
            self.index_source(index_name, reset=True)
            self.finish_index(index_name)
            self.set_current(index_name)
            logger.info("Finish full reindex, new index %s", index_name)

        return self.index_source()
