# -*- coding: utf-8 -*-
from os import path, listdir
from time import mktime, time
from fnmatch import fnmatch
from iso8601 import parse_date
from munch import munchify
import simplejson as json
import re

from openprocurement.search.source import BaseSource, logger


re_postalCode = re.compile(r"\d\d\d\d\d")

class OcdsSource(BaseSource):
    """OCDS Source from json files
    """
    __doc_type__ = 'ocds'

    config = {
        'ocds_dir': 'ocds',
        'ocds_mask': 'ocds-*.json',
        'ocds_speed': 500,
        'ocds_skip_until': None,
    }
    def __init__(self, config={}):
        if config:
            self.config.update(config)
        self.last_reset_time = 0
        self.last_files = []
        self.files = []

    def procuring_entity(self, item):
        return item['data'].get('procuringEntity', None)

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
        return munchify(data)

    def since_last_reset(self):
        return time() - self.last_reset_time

    def reset(self):
        files = []
        for name in listdir(self.config['ocds_dir']):
            if fnmatch(name, self.config['ocds_mask']):
                files.append(name)
        self.files = sorted(files)
        self.last_reset_time = time()
        logger.info("Reset ocds, found %d files", len(self.files))

    def lazy_reset(self):
        self.reset()
        # compare to last one, clean if noting was changed
        if self.files != self.last_files:
            self.last_files = list(self.files)
        else:
            self.files = []

    def items(self):
        if not self.files and self.since_last_reset() > 3600:
            self.lazy_reset()
        if not self.files:
            return
        self.last_skipped = None
        skip_until = self.config.get('ocds_skip_until', None)
        if skip_until and skip_until[:2] != '20':
            skip_until = None
        name = self.files.pop(0)
        fullname = path.join(self.config['ocds_dir'], name)
        with open(fullname) as f:
            data = json.load(f)
        for r in data['releases']:
            if self.should_exit:
                raise StopIteration()
            item = r['tender']
            if 'tenderID' not in item:
                item['tenderID'] = r['ocid']
            if 'dateModified' not in item:
                item['dateModified'] = r['date']
            if skip_until and skip_until > item['dateModified']:
                self.last_skipped = item['dateModified']
                continue
            yield self.patch_version(item)

    def get(self, item):
        return self.patch_tender(item)
