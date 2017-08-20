# -*- coding: utf-8 -*-
from logging import getLogger
from time import time, sleep, localtime, strftime
from restkit import request
from retrying import retry
import simplejson as json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import ElasticsearchException, NotFoundError

from openprocurement.search.utils import SharedFileDict

logger = getLogger(__name__)


class SearchEngine(object):
    """Search Engine
    """

    config = {
        'index_names': 'index_names',
        'elastic_host': 'localhost',
        'elastic_timeout': 300,
        'slave_mode': None,
        'slave_wakeup': 600,
        'check_on_start': 1,
        'bulk_insert': False,
        'update_wait': 5,
        'error_wait': 10,
        'start_wait': 1,
    }
    es_options = {
        'max_retries': 3,
        'retry_on_timeout': True,
        'sniff_on_start': True,
        'request_timeout': 300,
        'timeout': 300,
    }
    search_index_map = {}
    debug = False

    def __init__(self, config={}, role='search'):
        self.index_list = list()
        if config:
            self.config.update(config)
            self.config['update_wait'] = int(self.config['update_wait'])
        self.names_db = SharedFileDict(self.config.get('index_names'))
        self.elatic_host = self.config.get('elastic_host')
        if role and (role + '_elastic_host') in self.config:
            self.elatic_host = self.config[role + '_elastic_host']
        self.es_options['timeout'] = int(self.config['elastic_timeout'])
        self.es_options['request_timeout'] = int(self.config['elastic_timeout'])
        self.elastic = Elasticsearch([self.elatic_host],
            **self.es_options)
        self.slave_mode = self.config.get('slave_mode') or None
        self.slave_wakeup = int(self.config['slave_wakeup'] or 600)
        self.debug = self.config.get('debug', False)
        self.bulk_buffer = dict()
        self.bulk_errors = False
        self.should_exit = False

    def init_search_map(self, search_map={}):
        if search_map:
            self.search_index_map.update(search_map)
        # update index_name from config.ini
        for k in self.search_index_map.keys():
            names = self.config.get('search_' + k)
            if not names:
                continue
            names = [s.strip() for s in names.split(',') if s]
            self.search_index_map[k] = names
        # update index names from rename_xxx
        for k, names in self.search_index_map.items():
            for i, index in enumerate(names):
                if hasattr(index, '__index_name__'):
                    names[i] = index.__index_name__
                new_name = self.config.get('rename_' + names[i])
                if new_name:
                    names[i] = new_name
        logger.debug("Search indexes %s", str(self.search_index_map))

    def start_in_subprocess(self):
        # create copy of elastic connection
        self.elastic = Elasticsearch([self.elatic_host],
            **self.es_options)
        # we're not master anymore, clear inherited reindex_process
        for index in self.index_list:
            if getattr(index, 'reindex_process', None):
                index.reindex_process = None

    def stop_childs(self):
        for index in self.index_list:
            if hasattr(index, 'stop_childs'):
                index.stop_childs()

    def add_index(self, index):
        if index not in self.index_list:
            self.index_list.append(index)

    def get_index(self, key):
        """Returns current index full name"""
        name = self.names_db.get(key)
        return name

    def set_index(self, key, name):
        self.names_db[key] = str(name)

    def get_current_indexes(self, index_keys=None):
        index_names = list()
        if not index_keys:
            index_keys = self.index_list
        for key in index_keys:
            if hasattr(key, '__index_name__'):
                key = key.__index_name__
            name = self.get_index(key)
            if not name:
                name = self.get_index(key + '.next')
            if name:
                index_names.append(name)
        return ','.join(index_names)

    def index_names_dict(self):
        self.names_db.read()
        return dict(self.names_db.cache or {})

    def index_docs_count(self):
        indices = IndicesClient(self.elastic)
        stin = indices.stats()
        stout = {}
        for k, v in self.index_names_dict().items():
            try:
                k += '_docs_count'
                stout[k] = stin['indices'][v]['primaries']['docs']['count']
            except KeyError:
                pass
        return stout

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def index_info(self, index_name):
        indices = IndicesClient(self.elastic)
        info = indices.get(index_name)
        return info[index_name]

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def index_stats(self, index_name):
        indices = IndicesClient(self.elastic)
        stats = indices.stats(index_name)
        return stats['indices'][index_name]['primaries']

    def search(self, body, start=0, limit=0, index=None, index_keys=None, index_set=None):
        if not index and index_set:
            index_keys = self.search_index_map[index_set]
        if not index:
            index = self.get_current_indexes(index_keys)
        if not index:
            return {"error": "current index not found"}
        if not limit:
            limit = 10
        if self.debug:
            logger.debug("SEARCH %s %d %d %s", index, start, limit, body)
        try:
            res = self.elastic.search(index=index,
                body=body, from_=start, size=limit)
        except ElasticsearchException as e:
            logger.error("elastic.search %s", str(e))
            res = {"error": unicode(e), "items": []}
        if 'hits' not in res:
            if 'error' in res:
                return res
            logger.error("elastic.search bad response")
            res = {"error": "bad response", "items": []}
            return res
        hits = res['hits']
        items = []
        if 'hits' in hits:
            for h in hits['hits']:
                items.append(h['_source'])
        res = {
            'items': items,
            'total': hits.get('total', 0),
            'start': start
        }
        return res

    def master_heartbeat(self, value=None):
        filename = "%s.heartbeat" % self.config.get('index_names')
        if value:
            if value - getattr(self, 'last_saved_heartbeat', 0) < 10:
                return value
            self.last_saved_heartbeat = value
            fp = open(filename, "w")
            fp.write("%d\n" % value)
            fp.close()
        else:
            fp = open(filename)
            value = fp.read() or 0
            value = int(value)
            fp.close()
        if self.debug:
            logger.debug("Heartbeat %s", str(value))
        return value

    def test_heartbeat(self):
        # set initial heartbeat_value to current time
        if not getattr(self, 'last_heartbeat_check', None):
            self.last_heartbeat_check = 1
            self.last_heartbeat_value = time()
        # cache response value for 60 sec
        if time() - self.last_heartbeat_check < 60:
            return self.last_heartbeat_value
        # ... or get from master
        try:
            r = request(self.slave_mode, timeout=5)
            data = json.loads(r.body_string())
            # log result
            hv = data['heartbeat']
            lag = time() - hv
            logger.info("Master heartbeat %s lag %s min",
                strftime('%H:%M:%S', localtime(hv)), int(lag / 60))
        except Exception as e:
            logger.error("Can't check heartbeat %s %s",
                type(e).__name__, unicode(e))
            # if request failed accept last successed value
            data = {'heartbeat': self.last_heartbeat_value}
        if 'index_names' in data:
            self.names_db.update(data['index_names'])
        self.last_heartbeat_check = time()
        self.last_heartbeat_value = int(data.get('heartbeat') or 0)
        return self.last_heartbeat_value


class IndexEngine(SearchEngine):
    """Indexer Engine
    """
    def __init__(self, config={}, role='index'):
        super(IndexEngine, self).__init__(config, role)
        logger.info("Start with config:\n\t%s", self.dump_config())

    def dump_config(self):
        cs = "\n\t".join(["%-17s = %s" % (k, v)
            for k, v in sorted(self.config.items())])
        return cs

    def dump_index_names(self):
        ns = "\n\t".join(["%-17s = %s" % (k, v)
            for k, v in self.index_names_dict().items()])
        return ns or "(index_names is empty)"

    def index_exists(self, index_name):
        try:
            self.index_info(index_name)
        except:
            return False
        return True

    def set_alias(self, alias_name, index_name):
        indices = IndicesClient(self.elastic)
        old_index = alias_name + '_20*'
        try:
            indices.delete_alias(index=old_index, name=alias_name)
        except NotFoundError:
            pass
        except Exception as e:
            logger.error("Alias %s for %s not created: %s", alias_name, index_name, str(e))
            return
        try:
            indices.put_alias(index=index_name, name=alias_name, body={})
        except Exception as e:
            logger.error("Alias %s for %s not created: %s", alias_name, index_name, str(e))
            return
        logger.info("Set alias %s -> %s", alias_name, index_name)

    def create_index(self, index_name, body):
        indices = IndicesClient(self.elastic)
        indices.create(index_name, body=body)

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def get_item(self, index_name, meta):
        try:
            found = self.elastic.get(index_name,
                doc_type=meta.get('doc_type'),
                id=meta['id'],
                _source=True)
        except NotFoundError:
            return None
        return found

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def test_exists(self, index_name, meta):
        try:
            found = self.elastic.get(index_name,
                doc_type=meta.get('doc_type'),
                id=meta['id'],
                _source=False)
        except NotFoundError:
            return False
        return found['_version'] >= meta['version']

    def index_item(self, index_name, item, ignore_bulk=False):
        # bulk insert
        if not ignore_bulk and self.config['bulk_insert']:
            return self.bulk_index(index_name, item)

        # signle insert
        meta = item['meta']
        retry_count = 0
        while True:
            try:
                res = self.elastic.index(index_name,
                    doc_type=meta.get('doc_type'),
                    id=meta['id'],
                    version=meta['version'],
                    version_type='external',
                    body=item['data'])
                return res
            except ElasticsearchException as e:
                if retry_count > 3:
                    raise e
                retry_count += 1
                logger.error("[%s] Can't index %s error %s",
                    index_name, str(meta), str(e))
                self.sleep(self.config['error_wait'])
                if self.test_exists(index_name, meta):
                    return None

        return None

    def bulk_index(self, index_name, item):
        if index_name not in self.bulk_buffer:
            self.bulk_buffer[index_name] = list()
        items_list = self.bulk_buffer[index_name]
        items_list.append(item)
        if len(items_list) >= 500:
            self.flush_bulk()
        return True

    def flush_bulk(self):
        for index_name, items_list in self.bulk_buffer.items():
            if len(items_list) < 50 or self.bulk_errors:
                for item in items_list:
                    if not self.test_exists(index_name, item['meta']):
                        self.index_item(index_name, item, ignore_bulk=True)
            else:
                bulk_items = [{
                    '_index': index_name,
                    '_type': item['meta']['doc_type'],
                    '_id': item['meta']['id'],
                    '_version': item['meta']['version'],
                    '_version_type': 'external',
                    '_source': item['data']

                } for item in items_list]
                try:
                    bulk_res = bulk(self.elastic, bulk_items,
                        request_timeout=self.es_options['request_timeout'],
                        timeout=self.es_options['timeout'])
                    logger.debug("[%s] BULK result %s", index_name, bulk_res)
                except ElasticsearchException as e:
                    logger.error("[%s] Error BULK index %s: %s",
                        index_name, type(e).__name__, str(e))
                    self.bulk_errors = True
                    return

        self.bulk_buffer = dict()
        self.bulk_errors = False

    def index_by_type(self, doc_type, item):
        for index in self.index_list:
            if index.source.doc_type == doc_type:
                if index.source.push(item):
                    index.index_source()
            break

    def heartbeat(self, source=None):
        """
        In master mode update timestamp and return true
        In slave mode update index_names from master
            and check maser last timestamp
            if age > slave_wakeup return true (allow slave working)
            if age < slave_wakeup return false and also reset source
        """
        if self.should_exit:
            return False

        try:
            self.master_heartbeat(int(time()))
        except Exception as e:
            logger.error("Can't update heartbeat %s", str(e))

        if self.slave_mode:
            heartbeat_value = self.test_heartbeat()
            heartbeat_diff = time() - heartbeat_value
            if heartbeat_diff > self.slave_wakeup:
                logger.warning("Master died %d min ago, go slave",
                    int(heartbeat_diff / 60))
                return True
            else:
                if source:
                    source.should_reset = True
                return False

        return True

    def sleep(self, seconds):
        if not isinstance(seconds, float):
            seconds = float(seconds)
        while not self.should_exit and seconds > 0:
            sleep(0.1 if seconds > 0.1 else seconds)
            seconds -= 0.1

    def wait_for_backend(self):
        if self.config['start_wait']:
            self.sleep(self.config['start_wait'])
        alive = False
        retry_count = 0
        while not alive:
            self.heartbeat()
            try:
                alive = self.elastic.info()
            except ElasticsearchException as e:
                if retry_count > 30:
                    raise e
                retry_count += 1
                logger.error(u"Failed get elastic info: %s", unicode(e))
                self.sleep(self.config['error_wait'])
            if self.should_exit:
                return

        info_string = "".join(["\n\t%-17s = %s" % (k, str(v))
                for k, v in alive['version'].items()])
        logger.info(u"ElasticSearch version: %s", info_string)

        if alive['version']['number'][:4] != '1.7.':
            raise RuntimeError("Unsuported elastic version, requie v1.7")

    def run(self):
        logger.info("Configured with indexes %s\n\t%s",
                    self.index_list, self.dump_index_names())

        self.wait_for_backend()

        if self.slave_mode:
            logger.info("* Start in slave mode")
        else:
            logger.info("Check current indexes")
            if self.config['check_on_start']:
                for index in self.index_list:
                    index.check_on_start()

        # start main loop
        allow_reindex = not self.slave_mode
        while not self.should_exit:
            for index in self.index_list:
                if self.should_exit:
                    break
                index.process(allow_reindex)
                self.flush_bulk()
                self.sleep(self.config['update_wait'])

        logger.info("Leave main loop")
