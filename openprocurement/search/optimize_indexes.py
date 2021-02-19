#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import yaml
import logging
import urllib2
import argparse
import simplejson as json
from socket import setdefaulttimeout
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from openprocurement.search.index import BaseIndex

from ConfigParser import ConfigParser

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logger = logging.getLogger(__name__)


class Options:
    start_time = 0
    finish_time = 0


def get_segments(indices, index_name):
    res = indices.segments(index_name, human=True)
    if isinstance(res, dict) and 'indices' in res:
        res = res['indices'][index_name]
        for k, s in res['shards'].items():
            for i in s:
                i.pop('segments', None)
                i.pop('routing', None)
    return res


def optimize_index(elastic_host, index_name, opts):
    setdefaulttimeout(opts.timeout)
    logger.info("OPTIMIZE %s", index_name)
    es_options = {
        'retry_on_timeout': False,
        'request_timeout': opts.timeout,
        'timeout': opts.timeout,
    }
    elastic = Elasticsearch([elastic_host], **es_options)
    indices = IndicesClient(elastic)
    try:
        res = indices.refresh(index_name)
        logger.info("Refresh %s result %s", index_name, str(res))
        time.sleep(5)
        res = get_segments(indices, index_name)
        logger.info("Segments %s before %s", index_name, str(res))
        res = indices.optimize(index_name, max_num_segments=opts.maxsegs)
        logger.info("Optimize %s result %s", index_name, str(res))
        time.sleep(5)
        res = get_segments(indices, index_name)
        logger.info("Segments %s after %s", index_name, str(res))
    except Exception as e:
        logger.error("Optimize failed %s", str(e))
    else:
        logger.info("SUCCESS %s", index_name)


def process_all(elhost, index_list, index_yaml, opts):
    fresh_time = time.time() - (opts.minage * 86400)
    candidates = list()

    current_keys = index_yaml.keys()
    current_names = index_yaml.values()

    for name in current_names:
        try:
            created_time = BaseIndex.index_created_time(name)
        except ValueError:
            logger.info("Skip unknown %s", name)
            continue
        if created_time < 1e9:
            logger.info("Skip bad name %s time %f", name, created_time)
            continue
        if created_time > fresh_time:
            logger.info("Skip fresh index %s", name)
            continue
        # optimize noindex first
        noindex_name = 'noindex_' + name
        if noindex_name in index_list:
            candidates.append(noindex_name)
        # then main index
        candidates.append(name)

    if len(candidates) < 1:
        logger.info("Not enought candidates")
        return

    for name in candidates:
        if time.time() >= Options.finish_time:
            logger.info("Stop by max working time")
            break
        optimize_index(elhost, name, opts)


def get_indexes(elastic_host):
    url = "http://%s/_cat/indices?format=json" % elastic_host
    resp = urllib2.urlopen(url)
    data = json.loads(resp.read())
    return data


def process_config(opts):
    logger.info("Process config %s", opts.config)

    parser = ConfigParser()
    parser.read(opts.config)

    elhost = parser.get('search_engine', 'elastic_host')
    index_list = get_indexes(elhost)

    logger.info("Found %d indexes on %s", len(index_list), elhost)

    yafile = parser.get('search_engine', 'index_names')
    with open("%s.yaml" % yafile) as f:
        index_yaml = yaml.safe_load(f)

    process_all(elhost, index_list, index_yaml, opts)


def main():
    parser = argparse.ArgumentParser(description='Optimize search indices.')
    parser.add_argument('config', help='search.ini config file')
    parser.add_argument('--minage', type=int, default=1,
        help='minimum index age in days (default: 1)')
    parser.add_argument('--maxsegs', type=int, default=1,
        help='maximum number of segments (default: 1)')
    parser.add_argument('--maxwork', type=int, default=5,
        help='maximum execution time in hours (default: 5)')
    parser.add_argument('--timeout', type=int, default=18000,
        help='default socket timeout sec (default: 18000)')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format=FORMAT)

    Options.start_time = time.time()
    Options.finish_time = Options.start_time + (3600 * args.maxwork)

    for config in sys.argv[1:]:
        if config.startswith('--'):
            continue
        process_config(args)


if __name__ == '__main__':
    main()
