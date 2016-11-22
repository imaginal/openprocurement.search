# -*- coding: utf-8 -*-
import sqlite3
from munch import munchify

from openprocurement.search.source import BaseSource

from logging import getLogger
logger = getLogger(__name__)


class OrgsSource(BaseSource):
    """Organisations fake source
    """
    __doc_type__ = 'org'

    config = {
        'orgs_db': None,
        'orgs_queue': 1000,
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
        item_data = item.get('data', {})
        data = {
            "edrpou": item['id'],
            "location": item_data.get('address', {}).get('region', u""),
            "name": (item_data.get('name') or item_data.get('name_ru') or
                     item_data.get('identifier', {}).get('legalName', u"")),
            "short": u"",
            "rank": 1,
        }
        if self.db_curs:
            self.db_curs.execute("SELECT name,short,loc FROM uo WHERE code=?",
                                 (item['id'],))
            row = self.db_curs.fetchone()
            if row:
                data['name'] = row[0]
                data['short'] = row[1]
                data['location'] = row[2]
            else:
                logger.info("UA-EDR not found '%s'", item['id'])
        return {'meta': item, 'data': data}

    def reset(self):
        self.queue = {}

    def push(self, item):
        """push item in to queue and return True if need to flush"""
        try:
            code = item['identifier']['id']
            if code and type(code) == int:
                code = str(code)
            if len(code) < 5 or len(code) > 15:
                raise ValueError("Bad code")
        except (KeyError, TypeError, ValueError):
            return False
        name = (item.get('name') or item.get('name_ru') or
                item.get('identifier', {}).get('legalName', u""))
        if not name or len(name) < 3:
            return False
        if code not in self.queue:
            data = {
                'id': code,
                'dateModified': "",
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
