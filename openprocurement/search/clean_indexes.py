#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import yaml
import logging
import urllib2
import simplejson as json
from ConfigParser import ConfigParser

FORMAT='%(asctime)-15s %(levelname)s %(processName)s %(message)s'
logger=logging.getLogger(__name__)


def delete_index(elastic_host, name):
    url = "http://%s/%s" % (elastic_host, name)
    logger.warning("DELETE %s" % url)
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url)
    request.get_method = lambda: 'DELETE'
    resp = opener.open(request)
    data = resp.read()
    if resp.getcode() != 200:
        logger.error("%d %s", resp.getcode(), data)


def process_index(elastic_host, index_list, prefix, current):
    logger.info("Process index prefix %s", prefix)

    fresh_time = time.time() - (10*24*3600) # 10 days
    candidates = list()
    skip_count = 0

    for index in index_list:
        name = index['index']
        if not name.startswith(prefix+'_'):
            logger.debug("Skip by prefix %s", name)
            continue
        if name == current:
            logger.info("Skip current %s", name)
            skip_count += 1
            continue
        name_prefix, created_time = name.rsplit('_', 1)
        if int(created_time) > fresh_time:
            logger.info("Skip fresh %s", name)
            skip_count += 1
            continue
        if name.startswith(prefix):
            candidates.append((name, created_time))

    if len(candidates) < 1:
        logger.info("Not enought candidates")
        return

    if skip_count < 2:
        candidates = sorted(candidates, key=lambda i: i[1])
        name, created = candidates.pop()
        logger.info("Skip youngest %s", name)

    for name,created in candidates:
        delete_index(elastic_host, name)


def get_indexes(elastic_host):
    url = "http://%s/_cat/indices?format=json" % elastic_host
    resp = urllib2.urlopen(url)
    data = json.loads(resp.read())
    return data


def process_config(config):
    logger.info("Process config %s", config)

    parser = ConfigParser()
    parser.read(config)

    elhost = parser.get('search_engine', 'elastic_host')
    index_list = get_indexes(elhost)

    yafile = parser.get('search_engine', 'index_names')
    with open("%s.yaml" % yafile) as f:
        index_names = yaml.load(f)

    for prefix in index_names:
        process_index(elhost, index_list, prefix, index_names[prefix])


def main():
    if len(sys.argv) < 2:
        print "usage: clean_indexes search.ini [other.ini ...]"
        return

    logging.basicConfig(level=logging.INFO, format=FORMAT)

    for config in sys.argv[1:]:
        process_config(config)


if __name__ == '__main__':
    main()
