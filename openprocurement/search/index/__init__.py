# -*- coding: utf-8 -*-
import os, sys
from time import time, sleep
from logging import getLogger
from multiprocessing import Process

logger = getLogger(__name__)


class BaseIndex(object):
    """Search Index Interface
    """
    config = {
        'async_reindex': 0,
        'ignore_errors': 0,
        'index_speed': 100,
        'error_wait': 10,
    }
    allow_async_reindex = False
    magic_exit_code = 84
    reindex_process = None
    next_index_name = None
    last_current_index = None

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
        if self.allow_async_reindex:
            self.allow_async_reindex = self.config['async_reindex']
        self.after_init()

    def __del__(self):
        self.stop_childs()

    def __str__(self):
        return self.__index_name__

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

    def after_init(self):
        pass

    def need_reindex(self):
        return not self.current_index

    def create_index(self, name):
        return

    def new_index(self, is_async=False):
        index_key = self.__index_name__
        index_key_next = "{}.next".format(index_key)
        # try restore last index (in case of crash)
        name = self.engine.get_index(index_key_next)
        current_index = self.current_index
        if current_index and name <= current_index:
            name = None
        if name and self.index_age(name) > 5 * 24 * 3600:
            name = None
        if not name:
            name = "{}_{}".format(index_key, int(time()))
            self.create_index(name)
            self.engine.set_index(index_key_next, name)
        else:
            logger.info("Use already created index %s", name)
        # check current not same to new
        assert name != current_index, "same index name"
        return name

    def delete_index(self, name):
        index_key = self.__index_name__
        index_key_prev = "{}.prev".format(index_key)
        self.engine.set_index(index_key_prev, name)

    def set_current(self, name):
        if self.engine.should_exit:
            return
        index_key = self.__index_name__
        old_index = self.current_index
        if name != old_index:
            logger.info("Change current %s index %s -> %s",
                        index_key, old_index, name)
            self.engine.set_index(index_key, name)
            # assert(self.current_index == name)
            if old_index:
                self.delete_index(old_index)
        # remove index.next key
        index_key_next = "{}.next".format(index_key)
        if self.engine.get_index(index_key_next) == name:
            self.engine.set_index(index_key_next, '')
        return name

    def test_exists(self, index_name, info):
        return self.engine.test_exists(index_name, info)

    def test_noindex(self, item):
        return False

    def before_index_item(self, item):
        return True

    def handle_error(self, error, exc_info):
        if self.config['ignore_errors']:
            logger.error("%s (ignored)", str(error))
        else:
            raise exc_info[0], exc_info[1], exc_info[2]

    def indexing_stat(self, index_name, fetched, indexed, iter_count, last_date):
        last_date = last_date or ""
        pause = 1.0 * iter_count / self.config['index_speed']
        logger.info(
            "[%s] Fetched %d indexed %d last %s", # wait %1.1fs
            index_name, fetched, indexed, last_date)
        if pause > 0.01:
            self.engine.sleep(pause)

    def index_item(self, index_name, item):
        if self.test_noindex(item):
            if self.engine.debug:
                logger.debug("[%s] Noindex %s %s", index_name,
                             item['data']['id'], 
                             item['data'].get('tenderID', ''))
            return None
    
        self.before_index_item(item)

        return self.engine.index_item(index_name, item)

    def index_source(self, index_name=None, reset=False):
        if not index_name:
            # also check maybe current index was changed
            if self.current_index != self.last_current_index:
                self.last_current_index = self.current_index
                reset = True
            index_name = self.current_index

        if not index_name and self.engine.slave_mode:
            if not self.engine.heartbeat(self.source):
                return
            index_name = self.current_index

        if not index_name:
            if not self.reindex_process:
                logger.warning("No current index for %s", repr(self))
            return

        if reset:
            self.source.reset()

        index_count = 0
        total_count = 0
        # heartbeat always True in master mode
        # heartbeat return True in slave mode only if master fail
        while self.engine.heartbeat(self.source):
            info = {}
            iter_count = 0
            for info in self.source.items():
                if self.engine.should_exit:
                    return
                if not self.test_exists(index_name, info):
                    try:
                        item = self.source.get(info)
                        if self.index_item(index_name, item):
                            index_count += 1
                    except Exception as e:
                        self.handle_error(e, sys.exc_info())
                # update statistics
                iter_count += 1
                total_count += 1
                # update heartbeat for long indexing
                if iter_count >= 100:
                    self.indexing_stat(
                        index_name, total_count, index_count,
                        iter_count, info.get('dateModified'))
                    self.engine.heartbeat(self.source)
                    iter_count = 0

            # break if nothing iterated
            if iter_count > 0:
                self.indexing_stat(index_name, total_count, index_count,
                    iter_count, info.get('dateModified'))
            elif getattr(self.source, 'last_skipped', None):
                logger.info("[%s] Fetched %d, last_skipped %s",
                    index_name, total_count, self.source.last_skipped)
            elif not info:
                break

        if self.engine.should_exit:
            return

        return index_count

    def stop_childs(self):
        if self.source:
            self.source.should_exit = True
        if not self.reindex_process:
            return
        if self.reindex_process.pid == os.getpid():
            return
        logger.info("Terminate subprocess %s pid %d", 
            self.reindex_process.name, self.reindex_process.pid)
        try:
            self.reindex_process.terminate()
        except (AttributeError, OSError):
            pass

    def check_subprocess(self):
        if self.reindex_process:
            self.reindex_process.join(1)
        if not self.reindex_process or self.reindex_process.is_alive():
            return
        if self.reindex_process.exitcode == self.magic_exit_code:
            logger.info("Reindex-%s subprocess success, reset source",
                self.__index_name__)
            if self.next_index_name:
                self.set_current(self.next_index_name)
                self.next_index_name = None
            self.source.reset()
        else:
            logger.error("Reindex subprocess fail, exitcode = %d",
                self.reindex_process.exitcode)
        # close process
        self.reindex_process = None

    def async_reindex(self):
        logger.info("*** Start Reindex-%s in subprocess",
            self.__index_name__)
        # reconnect elatic and prevent future stop_childs
        self.engine.start_subprocess()

        if self.index_source(self.next_index_name, reset=True):
            exit_code = self.magic_exit_code
        else:
            exit_code = 1

        # exit with specific code to signal master process reset source
        logger.info("*** Exit subprocess")
        sys.exit(exit_code)

    def reindex(self):
        if self.engine.should_exit:
            return

        # check reindex process is alive
        if self.reindex_process and self.reindex_process.is_alive():
            return

        logger.info("Need reindex %s", self.__index_name__)
        # create new index and save name
        self.next_index_name = self.new_index()

        # reindex in old-way sync mode
        if not self.allow_async_reindex:
            if self.index_source(self.next_index_name, reset=True):
                self.set_current(self.next_index_name)
            return

        # reindex in async mode, start new reindex process
        proc_name = "Reindex-%s" % self.__index_name__
        self.reindex_process = Process(
            target=self.async_reindex, 
            name=proc_name)
        self.reindex_process.start()
        # wait for child
        retry_count = 0
        while not self.reindex_process.is_alive() and retry_count < 30:
            self.engine.sleep(1)
            retry_count += 1
        # check child is alive
        if self.reindex_process.is_alive():
            logger.info("Subprocess started %s pid %d", 
                self.reindex_process.name, self.reindex_process.pid)
        else:
            logger.error("Can't start subprocess")

    def process(self, allow_reindex=True):
        if self.engine.should_exit:
            return

        if self.reindex_process:
            self.check_subprocess()

        if self.need_reindex() and allow_reindex:
            self.reindex()

        return self.index_source()
