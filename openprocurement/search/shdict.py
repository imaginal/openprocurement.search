# -*- coding: utf-8 -*-
import os
import yaml
from time import time


class shdict:
    """dict shared between processes
    """
    def __init__(self, name, expire=5):
        self.cache = dict()
        self.filename = name + '.yaml'
        self.lastsync = 0
        self.expire = expire

    def __setitem__(self, key, value):
        if self.cache.get(key) == value:
            return
        if value:
            self.cache[key] = value
        else:
            self.cache.pop(key, None)
        self.write()

    def __getitem__(self, key):
        if self.is_expired():
            self.read()
        return self.cache[key]

    def get(self, key, default=None):
        if self.is_expired():
            self.read()
        return self.cache.get(key, default)

    def pop(self, key, default=None):
        return self.cache.pop(key, default)

    def is_expired(self):
        return time() - self.lastsync > self.expire

    def read(self):
        try:
            with open(self.filename) as f:
                self.cache = yaml.load(f) or {}
            self.lastsync = time()
        except (IOError, ValueError):
            pass

    def write(self):
        tmp_file = self.filename+'.tmp'
        with open(tmp_file, 'w') as f:
            yaml.dump(self.cache, f,
                default_flow_style=False)
        os.rename(tmp_file, self.filename)
        self.lastsync = time()
