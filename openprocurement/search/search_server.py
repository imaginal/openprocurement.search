# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import sys
from ConfigParser import ConfigParser

from flask import Flask, request, jsonify
from openprocurement.search.engine import SearchEngine


search_server = Flask(__name__)
search_server.config.from_object(__name__)

if len(sys.argv) > 2 and sys.argv[1] == '--paste':
    config_parser = ConfigParser()
    config_parser.read(sys.argv[2])
    search_config = dict(config_parser.items('search_engine'))
else:
    search_config = {}

search_engine = SearchEngine(search_config)
search_engine.add_index('tenders')
search_engine.add_index('ocds')


def match_query(query, field, type_=None, operator=None, analyzer=None):
    count = len(query)
    query = " ".join(query)
    query = {"query": query}
    if count > 1 and operator:
        query["operator"] = operator
    if analyzer:
        query["analyzer"] = analyzer
    if type_:
        query["type"] = type_
    return {"match": {field: query}}


def prefix_query(query, field):
    body = []
    for q in query:
        body.append({"prefix": {field: q}})
    if len(body) == 1:
        return body[0]
    return {"bool": {"should": body}}


def range_query(query, field):
    body = []
    for q in query:
        if q.find('-') < 0:
            res = prefix_query([q], field)
            body.append(res)
        else:
            beg, end = q.split('-', 1)
            body.append({"range": {
                field: {"gte": beg, "lte": end}
                }})
    if len(body) == 1:
        return body[0]
    return {"bool": {"should": body}}


def dates_query(query, args):
    op, key = args
    body = {"range": {
        key: {
            op: query,
            "time_zone": "+2:00",
        }}}
    return body


prefix_map = {
    'cpv': 'items.classification.id',
    'dkpp': 'items.additionalClassifications.id',
}
match_map = {
    'tid': 'tenderID',
    'edrpou': 'procuringEntity.identifier.id',
    'procedure': 'procurementMethod',
    'proc_type': 'procurementMethodType',
    'status': 'status',
}
range_map = {
    'region': 'procuringEntity.address.postalCode',
}
dates_map = {
    'auction_start': ('gte', 'auctionPeriod.endDate'),
    'auction_end':   ('lte', 'auctionPeriod.startDate'),
    'award_start':   ('gte', 'awardPeriod.endDate'),
    'award_end':     ('lte', 'awardPeriod.startDate'),
    'enquiry_start': ('gte', 'enquiryPeriod.endDate'),
    'enquiry_end':   ('lte', 'enquiryPeriod.startDate'),
    'tender_start':  ('gte', 'tenderPeriod.endDate'),
    'tender_end':    ('lte', 'tenderPeriod.startDate'),
}
ftext_map = {
    'query': '_all',
}


@search_server.route('/search')
def search():
    args = request.args
    body = list()

    # hierarchical classifiers
    for key in prefix_map.keys():
        if not args.get(key):
            continue
        field = prefix_map[key]
        query = args.getlist(key)
        match = prefix_query(query, field)
        body.append(match)

    # ID's and states
    for key in match_map.keys():
        if not args.get(key):
            continue
        field = match_map[key]
        query = args.getlist(key)
        match = match_query(query, field,
            operator='or',
            analyzer='whitespace')
        body.append(match)

    # range values ie postal code
    for key in range_map.keys():
        if not args.get(key):
            continue
        field = range_map[key]
        query = args.getlist(key)
        match = range_query(query, field)
        body.append(match)

    # date range
    for key in dates_map.keys():
        if not args.get(key):
            continue
        field = dates_map[key]
        query = args.get(key)
        match = dates_query(query, field)
        body.append(match)

    # full-text search
    for key in ftext_map.keys():
        if not args.get(key):
            continue
        field = ftext_map[key]
        query = args.getlist(key)
        match = match_query(query, field)
        body.append(match)

    if not body:
        return jsonify({"error": "Empty query"})
    elif len(body) == 1:
        body = {"query": body[0]}
    else:
        body = {"query": {"bool" : {"must" : body}}}

    body["sort"] = {"dateModified": {"order": "desc"}}

    start = int(args.get('start') or 0)
    res = search_engine.search(body, start)
    res['body'] = body
    return jsonify(res)


def make_app(global_conf, **kwargs):
    class config:
        pass
    for key, value in kwargs.items():
        setattr(config, key.upper(), value)
    search_server.config.from_object(config)
    return search_server
