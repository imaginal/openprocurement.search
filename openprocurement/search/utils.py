# -*- coding: utf-8 -*-
import os
import fcntl
import yaml
from time import time


class SharedFileDict(object):
    """dict shared between processes
    """
    def __init__(self, name, expire=1):
        self.cache = dict()
        self.filename = name + '.yaml'
        self.lastsync = 0
        self.expire = expire

    def __setitem__(self, key, value):
        if self.cache.get(key) == value:
            return
        if value:
            self.cache[key] = value
            self.write()
        else:
            self.cache.pop(key, None)
            self.write(key)

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

    def update(self, items):
        self.cache = dict(items)
        self.write(reread=False)

    def is_expired(self):
        return time() - self.lastsync > self.expire

    def read(self):
        try:
            with open(self.filename) as fp:
                self.cache = yaml.load(fp) or {}
            self.lastsync = time()
        except (IOError, ValueError):
            pass

    def write(self, pop_key=None, reread=True):
        tmp_file = self.filename+'.tmp'
        with open(tmp_file, 'w') as fp:
            fcntl.lockf(fp, fcntl.LOCK_EX)
            if reread:
                tmp_cache = self.cache
                self.read()
                self.cache.update(tmp_cache)
            if pop_key:
                self.cache.pop(pop_key)
            yaml.dump(self.cache, fp,
                default_flow_style=False)
            fcntl.lockf(fp, fcntl.LOCK_UN)
        os.rename(tmp_file, self.filename)
        # self.lastsync = time()
