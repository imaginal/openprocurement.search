# -*- coding: utf-8 -*-
import sqlite3
from munch import munchify

from openprocurement.search.source import BaseSource, logger


class OrgsSource(BaseSource):
    """Organisations fake source
    """
    __doc_type__ = 'org'

    config = {
        'orgs_db': None,
        'orgs_queue': 10,
    }
    def __init__(self, config={}):
        if config:
            self.config.update(config)
        if self.config['orgs_db']:
            logger.info("Open UA-EDR database %s", self.config['orgs_db'])
            self.db_conn = sqlite3.connect(self.config['orgs_db'])
            self.db_curs = self.db_conn.cursor()
        else:
            self.db_conn = None
            self.db_curs = None
        self.queue_size = int(self.config['orgs_queue'])
        self.queue = {}

    def patch_item(self, item):
        data = {
            "edrpou": item.id,
            "location": item.data.get('address', {}).get('region', u""),
            "name": (item.data.get('name') or item.data.get('name_ru') or
                item.data.get('identifier', {}).get('legalName', u"")),
            "short": u"",
            "rank": 1,
        }
        if self.db_curs:
            self.db_curs.execute("SELECT name,short,loc FROM uo WHERE code=?", (item['id'],))
            row = self.db_curs.fetchone()
            if row:
                data['name'] = row[0]
                data['short'] = row[1]
                data['location'] = row[2]
            else:
                logger.warning("UA-EDR not found %s", item['id'])
        return {'meta': item, 'data': data,}

    def reset(self):
        self.queue = {}

    def push(self, item):
        """push item in to queue and return True if need to flush"""
        code = item.get('identifier', {}).get('id', None)
        if not code or len(code) < 5 or len(code) > 15:
            return False
        if code not in self.queue:
            data = {
                'id': code,
                'dateModified': "-",
                'doc_type': self.doc_type,
                'version': 1L,
                'data': item,
            }
            self.queue[code] = munchify(data)
        if len(self.queue) >= self.queue_size:
            return True
        return False

    def items(self, name=None):
        for item in self.queue.values():
            yield item
        self.reset()

    def get(self, item):
        return self.patch_item(item)
