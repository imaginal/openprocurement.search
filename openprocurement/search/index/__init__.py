# -*- coding: utf-8 -*-
import os
import sys
import time
import simplejson as json
from datetime import datetime, timedelta
from multiprocessing import Process
from pkgutil import get_data
from logging import getLogger

logger = getLogger(__name__)


class BaseIndex(object):
    """Search Index Interface
    """
    config = {
        'async_reindex': 1,
        'ignore_errors': 0,
        'index_parallel': 1,
        'index_speed': 500,
        'query_speed': 100,
        'reindex_check': '1000,1',
        # common mappings settings
        'dynamic': False,
        'dynamic_templates': True,
        'date_detection': False,
        'numeric_detection': False,
        'number_of_shards': 6
    }
    allow_async_reindex = False
    force_next_reindex = False
    magic_exit_code = 84
    check_all_field = True
    skip_check_count = False
    reindex_process = None
    next_index_name = None
    last_current_index = None
    source_last_queries = 0
    plugin_config_key = ''

    SUFFIX_FORMAT = "%Y-%m-%d-%H%M%S"

    def __init__(self, engine, source, config={}):
        assert(self.__index_name__)
        logger.debug("Create %s index based on %s source", self, source)
        if config:
            self.config.update(config)
            self.config['index_speed'] = float(self.config['index_speed'])
            self.config['query_speed'] = float(self.config['query_speed'])
        rename_key = 'rename_' + self.__index_name__
        if rename_key in self.config:
            self.__index_name__ = self.config[rename_key]
        if self.allow_async_reindex:
            self.allow_async_reindex = self.config['async_reindex']
        self.set_reindex_options(self.config.get('reindex', ''),
            self.config.get('reindex_check', ''))
        source.engine = engine
        self.source = source
        self.engine = engine
        engine.add_index(self)
        engine.config.update(self.config)
        engine.config.update(source.config)
        self.load_plugins()
        self.after_init()

    def __del__(self):
        self.stop_childs()

    def __str__(self):
        return self.__index_name__

    def __repr__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.__index_name__)

    @classmethod
    def name(klass):
        return klass.__index_name__

    @staticmethod
    def index_created_time(name):
        prefix, suffix = name.rsplit('_', 1)
        try:
            s_time = time.strptime(suffix, BaseIndex.SUFFIX_FORMAT)
            suffix = time.mktime(s_time)
        except:
            suffix = 0
        return suffix

    @property
    def current_index(self):
        key = self.__index_name__
        return self.engine.get_index(key)

    def index_age(self, name=None):
        if not name:
            name = self.current_index
        if not name:
            return time.time()
        suffix = BaseIndex.index_created_time(name)
        return int(time.time() - int(suffix))

    def set_reindex_options(self, reindex_period, reindex_check):
        # reindex_period - two digits, first is age in days, seconds is weekday
        if reindex_period:
            self.max_age, self.reindex_day = map(int, reindex_period.split(','))
            self.max_age *= 86400
        # reindex_check - two digits, first is min docs count, second is max age of last doc
        if reindex_check:
            self.rc_mindocs, self.rc_max_age = map(int, reindex_check.split(','))
            self.rc_max_age *= 86400

    def load_plugins(self):
        self.plugins = []
        if not self.plugin_config_key:
            return
        plugins = self.config.get(self.plugin_config_key, '').strip()
        if not plugins:
            return
        for name in plugins.split(','):
            try:
                logger.info("Load plugin %s for %s", name, self)
                cls = self.engine.load_plugin_class(name)
                self.plugins.append(cls(self.config))
            except Exception as e:
                logger.error(
                    "Can't load plugin %s form %s=%s error %s",
                    name, self.plugin_config_key, plugins, e)
                if not self.config['ignore_errors']:
                    raise

    def after_init(self):
        pass

    def need_reindex(self):
        return not self.current_index

    def create_index(self, name):
        return

    def create_tender_index(self, name, common, tender, lang_list, apply_plugins=True):
        logger.info("Create new index %s from %s %s %s", name, common, tender, lang_list)
        common = json.loads(get_data(__name__, common))
        tender = json.loads(get_data(__name__, tender))
        # prepare for merge
        mappings = common['mappings']['_doc_type_']
        settings = common['settings']
        # overwrite some settings
        if not self.config['dynamic_templates']:
            mappings.pop('dynamic_templates', None)
        for key in ['dynamic', 'date_detection', 'numeric_detection']:
            if self.config[key]:
                mappings[key] = True
        # merge
        doc_type = self.source.__doc_type__
        for k, v in mappings.items():
            if k in tender['mappings'][doc_type]:
                raise KeyError("Common key '%s' found in mappings" % k)
            tender['mappings'][doc_type][k] = v
        for k, v in settings.items():
            if k in tender['settings']:
                raise KeyError("Common key '%s' found in settings" % k)
            tender['settings'][k] = v
        # apply lang
        for index_lang in lang_list:
            analysis = tender['settings']['index']['analysis']
            stopwords = 'stopwords_' + index_lang.strip()
            if stopwords in analysis['filter']:
                analysis['analyzer']['all_index']['filter'].append(stopwords)
                analysis['analyzer']['all_search']['filter'].append(stopwords)
            else:
                logger.warning("[%s] unknown language '%s' %s", name, index_lang, stopwords)
            stemmer = 'stemmer_' + index_lang.strip()
            if stemmer in analysis['filter']:
                analysis['analyzer']['all_index']['filter'].append(stemmer)
                analysis['analyzer']['all_search']['filter'].append(stemmer)
            else:
                logger.warning("[%s] unknown language '%s' %s", name, index_lang, stemmer)
        tender['settings']['index']['number_of_shards'] = int(self.config['number_of_shards'])
        # apply plugins to index mapping before create index
        if apply_plugins and self.plugins:
            for plugin in self.plugins:
                index_mappings = getattr(plugin, 'index_mappings', None)
                if index_mappings:
                    logger.info("Update mappings from plugin %s", plugin)
                    tender['mappings'][doc_type]['properties'].update(index_mappings)
                if callable(plugin.before_create_index):
                    logger.info("Call plugin.before_create_index %s", plugin)
                    plugin.before_create_index(index, name, tender)
        # create index
        self.engine.create_index(name, body=tender)

    def new_index(self, is_async=False):
        index_key = self.__index_name__
        index_key_next = "{}.next".format(index_key)
        # try restore last index (in case of crash)
        name = self.engine.get_index(index_key_next)
        current_index = self.current_index
        if current_index and name <= current_index:
            name = None
        if name and not name.startswith(index_key):
            name = None
        if name and not self.engine.index_exists(name):
            name = None
        if name and self.index_age(name) > 5 * 24 * 3600:
            name = None
        if name:
            logger.info("Use already created index %s", name)
        else:
            suffix = time.strftime(BaseIndex.SUFFIX_FORMAT)
            name = "{}_{}".format(index_key, suffix)
            self.create_index(name)
            self.engine.set_index(index_key_next, name)
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
            if self.check_index(name):
                self.engine.set_index(index_key, name)
            self.last_current_index = name
            # assert(self.current_index == name)
            if old_index:
                self.delete_index(old_index)
        # remove index.next key
        index_key_next = "{}.next".format(index_key)
        if self.engine.get_index(index_key_next) == name:
            self.engine.set_index(index_key_next, '')
        # set alias
        self.engine.set_alias(index_key, name)
        return name

    def from_cache(self, item):
        return item and item['meta'].get('from_cache')

    def test_exists(self, index_name, info):
        return self.engine.test_exists(index_name, info)

    def test_noindex(self, item):
        return False

    def before_index_item(self, item):
        return True

    def handle_error(self, error, exc_info):
        if self.config['ignore_errors']:
            logger.error("%s %s (ignored)", type(error).__name__, str(error))
        else:
            raise exc_info[0], exc_info[1], exc_info[2]

    def plugins_reset(self):
        for plugin in self.plugins:
            if callable(plugin.before_source_reset):
                plugin.before_source_reset(self)

    def source_reset(self, reset_plugins=True):
        if reset_plugins:
            self.plugins_reset()
        self.source.reset()

    def source_items(self):
        for plugin in self.plugins:
            if callable(plugin.before_source_items):
                for item in plugin.before_source_items(self):
                    yield item

        for item in self.source.items():
            yield item

    def indexing_stat(self, index_name, fetched, indexed, last_item={}, last_skipped=None):
        if not last_item and not last_skipped and fetched < 10 and indexed < 1:
            return
        if not last_item and last_skipped:
            last_item = {'dateModified': 'skipped ' + last_skipped}
        logger.info(
            "[%s] Fetched %d indexed %d last %s",
            index_name, fetched, indexed, last_item.get('dateModified', '-'))

    def indexing_wait(self, query_count, index_count):
        p1 = float(query_count) / self.config['query_speed']
        p2 = float(index_count) / self.config['index_speed']
        if p1 + p2 > 10:
            logger.info("Wait %1.1f sec.", max(p1, p2))
        self.engine.sleep(max(p1, p2))

    def index_item(self, index_name, item):
        if not item.get('meta') or not item.get('data'):
            logger.error("[%s] No data %s", index_name, str(item))
            return None
        if item['meta']['dateModified'] != item['data']['dateModified']:
            logger.error("[%s] dateModified mismatch %s", index_name, str(item))
            return None
        if self.test_noindex(item):
            if self.engine.debug:
                logger.debug("[%s] Noindex %s %s", index_name,
                             item['data'].get('id', ''),
                             item['data'].get('tenderID', ''))
            return None

        self.before_index_item(item)

        for plugin in self.plugins:
            if callable(plugin.before_index_item):
                plugin.before_index_item(self, item)

        return self.engine.index_item(index_name, item)

    def index_source(self, index_name=None, reset=False, reindex=False):
        if self.engine.slave_mode:
            if not self.engine.heartbeat(self.source):
                self.engine.sleep(10)
                return

        if not index_name:
            # also check maybe current index was changed
            if self.last_current_index != self.current_index:
                self.last_current_index = self.current_index
                reset = True
            index_name = self.current_index

        if not index_name:
            if not self.reindex_process:
                logger.warning("No current index for %s", repr(self))
            return

        if reset or self.source.need_reset():
            self.source_reset()

        for plugin in self.plugins:
            if callable(plugin.before_index_source):
                plugin.before_index_source(self)

        query_count = 0
        index_count = 0
        index_cprev = 0
        total_count = 0

        # heartbeat always True in master mode
        # heartbeat return True in slave mode only if master fail
        while self.engine.heartbeat(self.source):
            info = {}
            item = {}
            last_skipped = {}
            iter_count = 0

            for info in self.source_items():
                if self.engine.should_exit:
                    break
                if not self.test_exists(index_name, info):
                    try:
                        item = self.source.get(info)
                        if not self.from_cache(item):
                            query_count += 1
                        if self.index_item(index_name, item):
                            index_count += 1
                    except Exception as e:
                        self.handle_error(e, sys.exc_info())
                # update statistics
                total_count += 1
                iter_count += 1
                # update heartbeat for long indexing
                if iter_count >= 1000:
                    self.engine.flush_bulk()
                    self.indexing_stat(index_name, total_count, index_count, info)
                    iter_count = 0
                # take a break
                if query_count >= 10 or index_count - index_cprev >= 10:
                    self.indexing_wait(query_count, index_count - index_cprev)
                    index_cprev = index_count
                    query_count = 0
                # check for heartbeat in long term ops
                if iter_count == 500 and not self.engine.heartbeat(self.source):
                    break

            if item or iter_count:
                self.engine.flush()

            if self.engine.should_exit:
                return
            if not info:
                last_skipped = getattr(self.source, 'last_skipped', None)
            # break if nothing iterated
            if iter_count or last_skipped:
                self.indexing_stat(index_name, total_count, index_count, info, last_skipped)
            elif not info:
                break
            # break on each iteration if not in full reindex mode
            if not reindex and self.config['index_parallel']:
                logger.debug("[%s] Switch queue (index_parallel)", index_name)
                break

        # print source statistics
        if self.source.stat_queries - self.source_last_queries >= 100:
            logger.info("[%s:%s] API client %d resets, %d queries, %d fetched, %d skipped, %d loaded",
                        self.__index_name__, self.source.__doc_type__, self.source.stat_resets,
                        self.source.stat_queries, self.source.stat_fetched,
                        self.source.stat_skipped, self.source.stat_getitem)
            self.source_last_queries = self.source.stat_queries

        return index_count

    def stop_childs(self):
        if self.source:
            self.source.should_exit = True
        if not self.reindex_process or not self.reindex_process.pid:
            return
        if self.reindex_process.pid == os.getpid():
            return
        logger.info("Terminate subprocess %s pid %s",
            self.reindex_process.name, str(self.reindex_process.pid))
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
            logger.info("Reindex-%s subprocess pid %s success, reset source",
                self.__index_name__, str(self.reindex_process.pid))
            if self.next_index_name:
                self.set_current(self.next_index_name)
                self.next_index_name = None
            self.source_reset()
        else:
            logger.error("Reindex-%s subprocess pid %s fail, exitcode = %d",
                self.__index_name__,
                str(self.reindex_process.pid),
                self.reindex_process.exitcode)
        # close process
        self.reindex_process = None

    def check_index(self, index_name, wait=0):
        if not index_name or self.engine.should_exit:
            return False

        if wait:
            self.engine.sleep(wait)

        # check index mappings by check _all field
        if self.check_all_field:
            try:
                info = self.engine.index_info(index_name)
                stat = self.engine.index_stats(index_name)
            except Exception as e:
                logger.error("[%s] Check index failed: %s", index_name, str(e))
                self.force_next_reindex = True
                return False
            doc_type = self.source.__doc_type__
            if '_all' not in info['mappings'][doc_type]:
                logger.error("[%s] Check index failed: _all field not found, please reindex!",
                    index_name)
                self.force_next_reindex = True
                return False

        if self.skip_check_count or not self.rc_mindocs:
            if self.check_all_field:
                logger.info("[%s] Total docs %d", index_name, stat['docs']['count'])
            return True

        # check index docs count afetr reindex
        body = {
            "query": {"match_all": {}},
            "sort": {"dateModified": {"order": "desc"}}
        }
        try:
            res = self.engine.search(body, start=0, limit=1, index=index_name)
        except Exception as e:
            logger.error("[%s] match_all error %s", index_name, str(e))
            res = None
        if not res or not res.get('items'):
            logger.error("[%s] Check failed: empty or corrupted index", index_name)
            return False

        logger.info("[%s] Total docs %d, last indexed %s",
            index_name, res['total'], res['items'][0]['dateModified'])

        if self.rc_mindocs and res['total'] < self.rc_mindocs:
            logger.error("[%s] Check index failed: not enought docs %d, required %d",
                index_name, res['total'], self.rc_mindocs)
            return False

        if self.rc_max_age:
            min_date = datetime.now() - timedelta(seconds=self.rc_max_age)
            iso_min_date = min_date.isoformat()
            last_indexed = res['items'][0]['dateModified']
            if last_indexed < iso_min_date:
                logger.error("[%s] Check index failed: last indexed is too old %s, "+
                    "required %s", index_name, last_indexed, iso_min_date)
                return False

        return True

    def check_on_start(self):
        if not self.current_index:
            return True
        if self.check_index(self.current_index):
            return True
        if not self.engine.index_exists(self.current_index):
            self.set_current('')

    def async_reindex(self):
        logger.info("*** Start Reindex-%s in subprocess", self.__index_name__)

        # reconnect elatic and prevent future stop_childs
        self.engine.start_in_subprocess()

        self.index_source(self.next_index_name, reset=True, reindex=True)

        self.engine.flush()

        if self.check_index(self.next_index_name, wait=5):
            logger.info("*** Exit subprocess (success)")
            exit_code = self.magic_exit_code
        else:
            logger.info("*** Exit subprocess (with errors)")
            exit_code = 1

        # exit with specific code to signal master process reset source
        sys.exit(exit_code)

    def reindex(self):
        # check reindex process is alive
        if self.reindex_process and self.reindex_process.is_alive():
            return
        # don't start reindex process when exiting
        if self.engine.should_exit:
            return

        # clear reindex flag
        if self.force_next_reindex:
            self.force_next_reindex = False

        logger.info("Need reindex %s", self.__index_name__)
        # create new index and save name
        self.next_index_name = self.new_index()

        # reindex in old-way sync mode
        if not self.allow_async_reindex:
            self.index_source(self.next_index_name, reset=True, reindex=True)
            if self.check_index(self.next_index_name):
                self.set_current(self.next_index_name)
                self.next_index_name = None
                return True
            return False

        # also notify plugins before fork
        for plugin in self.plugins:
            if callable(plugin.before_fork_process):
                plugin.before_fork_process(self)

        # reindex in async mode, start new reindex process
        proc_name = "Reindex-%s" % self.__index_name__
        self.reindex_process = Process(
            target=self.async_reindex,
            name=proc_name)
        self.reindex_process.daemon = True
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
