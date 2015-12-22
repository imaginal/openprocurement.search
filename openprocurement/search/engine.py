# -*- coding: utf-8 -*-
from time import sleep
from logging import getLogger

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import ElasticsearchException

from openprocurement.search import shdict

logger = getLogger(__name__)


class SearchEngine:
    """Search Engine
    """
    config = {
        'index_names': 'index_names',
        'elastic_host': 'localhost',
        'update_wait': 5,
    }
    def __init__(self, config={}):
        self.index_list = list()
        if config:
            self.config.update(config)
            self.config['update_wait'] = int(self.config['update_wait'])
        self.names_db = shdict.shdict(self.config.get('index_names'))
        self.elastic = Elasticsearch([self.config.get('elastic_host')])

    def perform_request(self, method, url, params=None, body=None):
        return self.elastic.transport.perform_request(self, method, url, params, body)

    def add_index(self, index):
        if index not in self.index_list:
            self.index_list.append(index)

    def get_index(self, key):
        """Returns current index full name"""
        return self.names_db.get(key)

    def set_index(self, key, name):
        self.names_db[key] = name

    def get_current_indexes(self):
        index_names = list()
        for key in self.index_list:
            name = self.get_index(key)
            if name:
                index_names.append(name)
        return ','.join(index_names)

    def search(self, body, start=0):
        index = self.get_current_indexes()
        if not index:
            return {"error": "there is no spoon"}
        try:
            res = self.elastic.search(index=index,
                body=body, from_=start)
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


class IndexEngine(SearchEngine):
    """Indexer Engine
    """
    def create_index(self, index_name, body):
        indices = IndicesClient(self.elastic)
        indices.create(index_name, body=body)

    def test_exists(self, index_name, meta):
        try:
            item = self.elastic.get(index_name,
                doc_type=meta.get('doc_type'),
                id=meta['id'],
                version=meta['version'],
                _source=False)
        except ElasticsearchException:
            return False
        return item['_version'] == meta['version']

    def index_item(self, index_name, item):
        meta = item['meta']
        logger.debug("Index item %s id=%s version=%ld",
            index_name, meta['id'], meta['version'])
        try:
            res = self.elastic.index(index_name,
                doc_type=meta.get('doc_type'),
                id=meta['id'],
                version=meta['version'],
                version_type='external',
                body=item['data'])
        except ElasticsearchException as e:
            logger.error(u"Failed index %s object %s: %s",
                index_name, meta['id'], unicode(e))
            res = None
        return res

    def run(self):
        while True:
            for index in self.index_list:
                index.process()
            sleep(self.config['update_wait'])
