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


prefix_map = {
    'cpv': 'items.classification.id',
    'dkpp': 'items.additionalClassifications.id',
}
match_map = {
    'tid': 'tenderID',
    'edrpou': 'procuringEntity.identifier.id',
    'procedure': 'procurementMethod',
    'status': 'status',
}


@search_server.route('/search')
def search():
    args = request.args
    body = list()

    for key in prefix_map.keys():
        if not args.has_key(key):
            continue
        field = prefix_map[key]
        query = args.getlist(key)
        match = prefix_query(query, field)
        body.append(match)

    for key in match_map.keys():
        if not args.has_key(key):
            continue
        field = match_map[key]
        query = args.getlist(key)
        match = match_query(query, field,
            operator='or',
            analyzer='whitespace')
        body.append(match)

    if args.has_key('query'):
        query = args.getlist('query')
        match = match_query(query, '_all')
        body.append(match)

    if len(body) == 1:
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
