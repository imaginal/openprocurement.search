# -*- coding: utf-8 -*-
import os
import fcntl
import yaml
import time
import pytz
import iso8601
import functools
import signal
import datetime
import logging
import threading
import warnings

TZ = pytz.timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')


def get_now(tz=TZ):
    return datetime.datetime.now(tz)


def long_version(dt, local_timezone=TZ):
    if not dt:
        return 0
    if isinstance(dt, (str, unicode)):
        dt = iso8601.parse_date(dt, default_timezone=None)
    if dt.tzinfo:
        dt = dt.astimezone(local_timezone)
    unixtime = time.mktime(dt.timetuple())
    return int(1e6 * unixtime + dt.microsecond)


def request_error(e, client=None):
    out = str(e)
    try:
        request = getattr(e, 'request', None)
        response = getattr(e, 'response', None)
        headers = getattr(response, 'headers', None)
        if headers:
            out += " Status: " + str(response.status_code)
            out += " Headers: " + str(headers)
        if request:
            url = getattr(request, 'url')
            headers = getattr(request, 'headers')
            out += " RequestHeaders: " + str(headers)
            out += " URL: " + str(url)
        elif client:
            params = getattr(client, 'params')
            prefix = getattr(client, 'prefix_path')
            out += " RequestParams: " + str(params)
            out += " URL: " + str(prefix)
    except Exception:
        pass
    return out


def restkit_error(e, client=None):
    warnings.warn("restkit is deprecated", DeprecationWarning, stacklevel=2)
    return request_error(e, client)


def retry(tries, delay=5, backoff=2, exceptions=Exception, logger=None):
    def deco_retry(f):
        @functools.wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                mtries -= 1
                try:
                    return f(*args, **kwargs)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except exceptions as exc:
                    if logger:
                        msg = "{} {}: {}. Rerty {} of {} wait {} sec.".format(f.__name__,
                            exc.__class__.__name__, exc, tries - mtries, tries, mdelay)
                        logger.warning(msg)
                    time.sleep(mdelay)
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry  # true decorator
    return deco_retry


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


class Watchdog:
    counter = 0
    timeout = 0


def watchdog_thread(logger):
    while True:
        Watchdog.counter += 1
        time.sleep(1)
        if Watchdog.counter >= Watchdog.timeout - 5:
            if logger:
                logger.warning("Watchdog counter %d", Watchdog.counter)
        if Watchdog.counter == Watchdog.timeout:
            if logger:
                logger.warning("Watchdog kill pid %d", os.getpid())
            os.kill(os.getpid(), signal.SIGTERM)
        if Watchdog.counter >= Watchdog.timeout + 5:
            if logger:
                logger.warning("Watchdog exit")
            os._exit(1)
            break


def update_watchdog(timeout):
    if Watchdog.timeout:
        Watchdog.save_timeout = Watchdog.timeout
        Watchdog.timeout = int(timeout)


def restore_watchdog():
    if Watchdog.timeout:
        Watchdog.timeout = Watchdog.save_timeout


def setup_watchdog(timeout, logger=None):
    if not timeout or int(timeout) < 10:
        return
    Watchdog.timeout = int(timeout)
    thread = threading.Thread(target=watchdog_thread, name='Watchdog', args=(logger,))
    thread.daemon = True
    thread.start()


def reset_watchdog():
    if Watchdog.timeout:
        Watchdog.counter = 0


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
        changed = False
        for k, v in items.items():
            if self.cache.get(k) != v:
                self.cache[k] = v
                changed = True
        if changed:
            self.write()

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
        tmp_file = self.filename + '.tmp'
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
