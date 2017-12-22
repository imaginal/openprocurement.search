# -*- coding: utf-8 -*-
import os
import fcntl
import yaml
import time
import logging


def restkit_error(e, client=None):
    out = str(e)
    try:
        response = getattr(e, 'response', None)
        headers = getattr(response, 'headers', None)
        if headers:
            out += " Status:" + str(response.status_int)
            out += " Headers:" + str(headers)
        if client:
            headers = getattr(client, 'headers')
            params = getattr(client, 'params')
            prefix = getattr(client, 'prefix_path')
            uri = getattr(client, 'uri')
            out += " RequestHeaders:" + str(headers)
            out += " RequestParams:" + str(params)
            out += " URI:%s%s" % (uri, prefix)
    except:
        pass
    return out


def decode_bool_values(config):
    for key, value in config.items():
        value = str(value).strip().lower()
        if value in ("1", "true", "yes", "on"):
            config[key] = 1
        elif value in ("0", "false", "no", "off"):
            config[key] = 0
    return config


def chage_process_user_group(config, logger=None):
    from pwd import getpwuid, getpwnam
    from grp import getgrgid, getgrnam
    if config.get('user', ''):
        uid = os.getuid()
        newuid = getpwnam(config['user'])[2]
        if uid != newuid:
            if uid != 0:
                if logger:
                    logger.error("Can't change user not from root")
                return
            if config.get('group', ''):
                newgid = getgrnam(config['group'])[2]
                os.setgid(newgid)
            os.setuid(newuid)
    if not logger:
        return
    uid = os.getuid()
    gid = os.getgid()
    euid = os.geteuid()
    egid = os.getegid()
    logger.info("Process real user/group %d/%d %s/%s", uid, gid, getpwuid(uid)[0], getgrgid(gid)[0])
    logger.info("Process effective user/group %d/%d %s/%s", euid, egid, getpwuid(euid)[0], getgrgid(egid)[0])


class InfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno < logging.WARNING


class InfoHandler(logging.StreamHandler):
    def __init__(self, *args, **kwargs):
        logging.StreamHandler.__init__(self, *args, **kwargs)
        self.addFilter(InfoFilter())


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
        return time.time() - self.lastsync > self.expire

    def read(self):
        try:
            with open(self.filename) as fp:
                self.cache = yaml.load(fp) or {}
            self.lastsync = time.time()
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
        # self.lastsync = time.time()
