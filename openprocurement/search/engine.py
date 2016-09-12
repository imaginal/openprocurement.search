# -*- coding: utf-8 -*-
from time import time, sleep
from logging import getLogger

import simplejson as json
from restkit import request
from retrying import retry

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import ElasticsearchException, RequestError

from openprocurement.search import shdict

logger = getLogger(__name__)


class SearchEngine(object):
    """Search Engine
    """
    config = {
        'index_names': 'index_names',
        'elastic_host': 'localhost',
        'slave_mode': None,
        'slave_wakeup': 300,
        'update_wait': 5,
    }
    def __init__(self, config={}):
        self.index_list = list()
        if config:
            self.config.update(config)
            self.config['update_wait'] = int(self.config['update_wait'])
        self.names_db = shdict.shdict(self.config.get('index_names'))
        self.elastic = Elasticsearch([self.config.get('elastic_host')])
        self.slave_mode = self.config.get('slave_mode') or None
        self.should_exit = False

    def perform_request(self, method, url, params=None, body=None):
        return self.elastic.transport.perform_request(self, method, url, params, body)

    def add_index(self, index):
        if index not in self.index_list:
            self.index_list.append(index)

    def get_index(self, key):
        """Returns current index full name"""
        return self.names_db.get(key)

    def set_index(self, key, name):
        self.names_db[key] = str(name)

    def get_current_indexes(self, index_keys=None):
        index_names = list()
        if not index_keys:
            index_keys = self.index_list
        for key in index_keys:
            if isinstance(key, object):
                key = repr(key)
            name = self.get_index(key)
            if name:
                index_names.append(name)
        return ','.join(index_names)

    def index_names_dict(self):
        self.names_db.read()
        return dict(self.names_db.cache or {})

    def search(self, body, start=0, limit=0, index=None, index_keys=None):
        if not index:
            index = self.get_current_indexes(index_keys)
        if not index:
            return {"error": "current index not found"}
        if not limit:
            limit = 10
        try:
            res = self.elastic.search(index=index,
                body=body, from_=start, size=limit)
        except ElasticsearchException as e:
            res = {"error": unicode(e)}
        if not res.has_key('hits'):
            return res
        hits = res['hits']
        items = []
        if hits.has_key('hits'):
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
            fp = open(filename, "w")
            fp.write("%d\n" % value)
        else:
            fp = open(filename)
            value = fp.read() or 0
            value = int(value)
        fp.close()
        logger.debug("Heartbeat %s", str(value))
        return value

    def test_heartbeat(self):
        # set initial heartbeat_value to current time
        if getattr(self, 'last_heartbeat_check', None) is None:
            self.last_heartbeat_check = 0
            self.last_heartbeat_value = int(time()) - 30
        # cache response value for 30 sec
        if time() - self.last_heartbeat_check < 30:
            return self.last_heartbeat_value
        # ... or get from master
        try:
            r = request(self.slave_mode, timeout=5)
            data = json.loads(r.body_string())
        except Exception as e:
            logger.error("Can't check heartbeat %s", unicode(e))
            # if request failed accept last successed value
            data = {'heartbeat': self.last_heartbeat_value}
        if 'index_names' in data:
            for key, name in data['index_names'].items():
                self.set_index(key, name)
        self.last_heartbeat_check = time()
        self.last_heartbeat_value = int(data.get('heartbeat') or 0)
        return self.last_heartbeat_value


class IndexEngine(SearchEngine):
    """Indexer Engine
    """
    def __init__(self, config={}):
        super(IndexEngine, self).__init__(config)
        logger.info("Start with config:\n\t%s", self.config_dump())

    def config_dump(self):
        cs = "\n\t".join(["{} = {}".format(k ,v) \
            for k, v in sorted(self.config.items())])
        return cs

    def create_index(self, index_name, body):
        indices = IndicesClient(self.elastic)
        indices.create(index_name, body=body)

    def get_item(self, index_name, meta):
        try:
            found = self.elastic.get(index_name,
                doc_type=meta.get('doc_type'),
                id=meta['id'],
                _source=True)
        except ElasticsearchException:
            return None
        return found

    def test_exists(self, index_name, meta):
        try:
            found = self.elastic.get(index_name,
                doc_type=meta.get('doc_type'),
                id=meta['id'],
                _source=False)
        except ElasticsearchException:
            return False
        return found['_version'] >= meta['version']

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def index_item(self, index_name, item):
        meta = item['meta']
        logger.debug("PUT index %s object %s version %ld",
            index_name, meta['id'], meta['version'])
        try:
            res = self.elastic.index(index_name,
                doc_type=meta.get('doc_type'),
                id=meta['id'],
                version=meta['version'],
                version_type='external',
                body=item['data'])
        except (ElasticsearchException, RequestError) as e:
            logger.error(u"Failed index %s object %s: %s",
                index_name, meta['id'], unicode(e))
            raise
        return res

    def index_by_type(self, doc_type, item):
        for index in self.index_list:
            if index.source.doc_type == doc_type:
                break
        if index.source.doc_type != doc_type:
            raise IndexError("doc_type %s not found", doc_type)
        if index.source.push(item):
            # flush the index queue
            index.index_source()

    def heartbeat(self, source=None):
        """
        In master mode update timestamp and return true
        In slave mode update index_names from master
            and check maser last timestamp
            if age > 5 min return true (allow slave working)
            if age < 5 min return false and also reset source
        """
        if self.should_exit:
            return False
        if self.slave_mode:
            heartbeat_diff = time() - self.test_heartbeat()
            if heartbeat_diff > int(self.config['slave_wakeup']):
                logger.warning("Master died %d min ago",
                    int(heartbeat_diff/60))
                if getattr(source, 'should_reset', False):
                    source.should_reset = False
                    source.reset()
            else:
                source.should_reset = True
                return False

        self.master_heartbeat(int(time()))
        return True

    def wait_for_backend(self):
        alive = False
        while not alive:
            try:
                alive = self.elastic.info()
            except ElasticsearchException as e:
                logger.error(u"Failed get elastic info: %s", unicode(e))
                sleep(self.config['update_wait'])

    def run(self):
        logger.info("Starting IndexEngine with indices %s",
            str(self.index_list))
        self.wait_for_backend()
        allow_reindex = not self.slave_mode
        while not self.should_exit:
            for index in self.index_list:
                index.process(allow_reindex)
            sleep(self.config['update_wait'])
