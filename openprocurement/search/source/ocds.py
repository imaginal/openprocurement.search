# -*- coding: utf-8 -*-
from os import path, listdir
from time import mktime, sleep
from datetime import datetime
from fnmatch import fnmatch
from iso8601 import parse_date
import simplejson as json
import re

from openprocurement.search.source import BaseSource


re_postalCode = re.compile(r"\d\d\d\d\d")

class OcdsSource(BaseSource):
    """OCDS Source from json files
    """
    __doc_type__ = 'ocds-tender'

    config = {
        'ocds_dir': 'ocds',
        'ocds_mask': 'ocds-tender-*.json',
        'ocds_speed': 100,
    }
    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.files = []

    def patch_version(self, item):
        """Convert dateModified to long version
        """
        item['doc_type'] = self.__doc_type__
        dt = parse_date(item['dateModified'])
        version = 1e6 * mktime(dt.timetuple()) + dt.microsecond
        item['version'] = long(version)
        return item

    def patch_tender(self, item):
        if 'startDate' not in item['tenderPeriod'] and 'endDate' in item['tenderPeriod']:
            item['tenderPeriod']['startDate'] = item['tenderPeriod']['endDate']
        if 'startDate' not in item['awardPeriod'] and 'endDate' in item['awardPeriod']:
            item['awardPeriod']['startDate'] = item['awardPeriod']['endDate']
        for i in item['items']:
            if 'classification' not in i:
                continue
            if i['classification']['scheme'] != 'CPV':
                if 'additionalClassifications' not in i:
                    i['additionalClassifications'] = []
                i['additionalClassifications'].append(i['classification'])
        if 'postalCode' not in item['procuringEntity']['address']:
            code = item['procuringEntity']['address']['streetAddress'][-5:]
            if re_postalCode.match(code):
                item['procuringEntity']['address']['postalCode'] = code
        meta = {
            'id': item['id'],
            'doc_type': item.pop('doc_type'),
            'version': item.pop('version'),
            }
        data = {
            'meta': meta,
            'data': item,
            }
        return data

    def reset(self):
        files = []
        for name in listdir(self.config['ocds_dir']):
            if fnmatch(name, self.config['ocds_mask']):
                files.append(name)
        self.files = sorted(files)

    def items(self):
        if not self.files:
            dt = datetime.now()
            if dt.hour in [6,18] and dt.minute > 58:
                self.reset()
        if not self.files:
            return
        name = self.files.pop(0)
        fullname = path.join(self.config['ocds_dir'], name)
        with open(fullname) as f:
            data = json.load(f)
        for r in data['releases']:
            item = r['tender']
            if 'tenderID' not in item:
                item['tenderID'] = r['ocid']
            if 'dateModified' not in item:
                item['dateModified'] = r['date']
            yield self.patch_version(item)

    def get(self, item):
        return self.patch_tender(item)
