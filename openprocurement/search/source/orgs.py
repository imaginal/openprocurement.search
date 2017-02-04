# -*- coding: utf-8 -*-
import os
import os.path
import sqlite3
from munch import munchify

from openprocurement.search.source import BaseSource

from logging import getLogger
logger = getLogger(__name__)


class OrgsDecoder(object):
    def __init__(self, config={}):
        self.db_conn = None
        self.db_curs = None
        self.q_cache = {}
        orgs_db_size = 0
        if config.get('orgs_db'):
            orgs_db_size = os.path.getsize(config['orgs_db'])
        if orgs_db_size < 10000:    # don't accept empty database
            logger.warning("%s not exists or empty", config['orgs_db'])
            return
        try:
            fd = os.open(config['orgs_db'], os.O_RDONLY)
            self.db_conn = sqlite3.connect('/dev/fd/%d' % fd)
            os.close(fd)
        except Exception as e:
            logger.warning("Can't open %s via os.open(/dev/fd) %s",
                config['orgs_db'], str(e))
            self.db_conn = sqlite3.connect(config['orgs_db'])
        self.db_curs = self.db_conn.cursor()

    def is_connected(self):
        return self.db_curs

    def __del__(self):
        self.close()

    def close(self):
        try:
            if self.db_conn:
                self.db_conn.close()
        except Exception as e:
            logger.error("OrgsDecoder.close %s", str(e))
        self.db_curs = None
        self.db_conn = None

    def query(self, code):
        if not self.db_curs or not code:
            return
        if type(code) != str:
            try:
                code = str(code)
            except (TypeError, UnicodeEncodeError):
                return
        while len(code) < 8:
            code = "0" + code
        if code in self.q_cache:
            # get cached row and increase hits count
            row = self.q_cache[code][0]
            self.q_cache[code][1] += 1
            return row
        limit = 3
        while len(self.q_cache) > 10000:
            for k, v in self.q_cache.items():
                if v[1] < limit:
                    self.q_cache.pop(k)
            logger.info("Purge orgs cache, limit %d, new len %d", limit, len(self.q_cache))
            limit += 1
        try:
            self.db_curs.execute("SELECT code,name,short,loc FROM uo WHERE code=?", (code,))
            row = self.db_curs.fetchone()
            if row and len(row) > 2:
                row = (row[0], row[1], row[2], row[3])
                self.q_cache[code] = [row, 1]
                return row
        except Exception as e:
            logger.error("OrgsDecoder.query %s: %s", code, str(e))
            self.db_curs = None
        return

    def patch_entity(self, entity):
        if not self.db_curs or not entity:
            return
        try:
            if 'registryRecord' in entity:
                return
            if 'identifier' not in entity:
                return
            if entity['identifier'].get('scheme') != 'UA-EDR':
                return
            code = entity['identifier'].get('id')
            row = self.query(code)
            if not row:
                return
            entity['registryRecord'] = {
                'edrpou': row[0],
                'name': row[1],
                'shortName': row[2],
                'location': row[3]
            }
        except Exception as e:
            logger.error("OrgsDecoder.patch_entity %s: %s", entity, str(e))


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
        if not self.config.get('orgs_db'):
            logger.warning("No UA-EDR database, orgs will not be decoded")
        self.orgs_db = None
        self.should_reset = True
        self.queue_size = int(self.config['orgs_queue'])
        self.not_found = 0
        self.found = 0
        self.queue = {}

    def need_reset(self):
        if self.should_reset and len(self.queue) == 0:
            return True
        if not self.orgs_db and self.config['orgs_db']:
            return True
        return False

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
        if self.orgs_db and item.get('id'):
            row = self.orgs_db.query(item['id'])
            if row:
                data['name'] = row[1]
                data['short'] = row[2]
                data['location'] = row[3]
                self.found += 1
            else:
                self.not_found += 1
                if self.not_found % 100 == 0:
                    logger.warning("OrgsDecoder not_found %d found %d",
                        self.not_found, self.found)
        return {'meta': item, 'data': data}

    def reset(self):
        if self.orgs_db:
            self.orgs_db.close()
        if self.config['orgs_db']:
            orgs_db_size = os.path.getsize(self.config['orgs_db'])
            logger.info("Open UA-EDR database %s size %d MB",
                self.config['orgs_db'], orgs_db_size / 1024000)
            self.orgs_db = OrgsDecoder(self.config)
            if not self.orgs_db.is_connected():
                logger.warning("No UA-EDR database, orgs will not be decoded")
                self.orgs_db = None
        self.should_reset = False

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
        self.queue = {}

    def get(self, item):
        return self.patch_item(item)
