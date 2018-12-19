#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import yaml
import logging
import urllib2
import simplejson as json
from openprocurement.search.index import BaseIndex

from ConfigParser import ConfigParser

FORMAT='%(asctime)-15s %(levelname)s %(message)s'
logger=logging.getLogger(__name__)


class Options:
    fresh_age = 15


def delete_index(elastic_host, name):
    url = "http://%s/%s" % (elastic_host, name)
    logger.warning("DELETE %s" % url)
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url)
    request.get_method = lambda: 'DELETE'
    try:
        resp = opener.open(request)
        data = resp.read()
        if resp.getcode() != 200:
            logger.error("%d %s", resp.getcode(), data)
    except Exception as e:
        logger.error("Got exception %s", e)


def process_all(elastic_host, index_list, index_yaml):
    fresh_time = time.time() - (Options.fresh_age * 86400)
    candidates = list()

    current_keys  = index_yaml.keys()
    current_names = index_yaml.values()

    for index in sorted(index_list):
        name = index['index']
        prefix, _ = name.rsplit('_', 1)
        if prefix not in current_keys:
            logger.info("Skip by prefix %s", name)
            continue
        if name in current_names:
            logger.info("Skip current %s", name)
            continue
        try:
            created_time = BaseIndex.index_created_time(name)
        except:
            logger.info("Skip unknown %s", name)
            continue
        if created_time < 1e9:
            logger.info("Skip bad name %s time %f", name, created_time)
            continue
        if created_time > fresh_time:
            days = (time.time() - created_time) / 86400
            logger.info("Skip fresh %s (%1.1f days)", name, days)
            continue
        candidates.append(name)
        noindex_name = 'noindex_' + name
        if noindex_name in index_list:
            candidates.append(noindex_name)

    if len(candidates) < 1:
        logger.info("Not enought candidates")
        return

    for name in candidates:
        delete_index(elastic_host, name)


def delete_all(elastic_host, index_list, prefix):
    logger.warning("Delete all by prefix %s", prefix)
    for index in index_list:
        name = index['index']
        if name.startswith(prefix+'_'):
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

    logger.info("Found %d indexes on %s", len(index_list), elhost)

    yafile = parser.get('search_engine', 'index_names')
    with open("%s.yaml" % yafile) as f:
        index_yaml = yaml.load(f)

    if len(sys.argv) > 2 and '--all' in sys.argv:
        for prefix in index_yaml.keys():
            delete_all(elhost, index_list, prefix)
        return

    process_all(elhost, index_list, index_yaml)


def main():
    if len(sys.argv) < 2 or '-h' in sys.argv:
        print "usage: clean_indexes [options] search.ini"
        print "\noptions:"
        print "\t--all\t\tdelete all existing indexes"
        print "\t--age=<days>\tchange min index age (default 15 days)"
        sys.exit(1)

    if '--all' in sys.argv:
        answer = raw_input("Confirm delete all indexes (yes/no): ")
        if answer != "yes":
            print "Not confirmed, exit."
            sys.exit(1)

    logging.basicConfig(level=logging.INFO, format=FORMAT)

    for option in sys.argv[1:]:
        if option.startswith('--age='):
            Options.fresh_age = int(option[6:])
            logger.info("Set min index age = %d days", Options.fresh_age)

    for config in sys.argv[1:]:
        if config.startswith('--'):
            continue
        process_config(config)


if __name__ == '__main__':
    main()
