# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import sys
from ConfigParser import ConfigParser

from flask import Flask, request, jsonify, abort
from openprocurement.search.engine import SearchEngine

# create Flask app

search_server = Flask(__name__)
search_server.config.from_object(__name__)

# load config

config_parser = ConfigParser()
if len(sys.argv) > 2 and sys.argv[1] == '--paste':
    config_parser.read(sys.argv[2])
elif len(sys.argv) > 1 and sys.argv[1][-4:] == '.ini':
    config_parser.read(sys.argv[1])
search_config = dict(config_parser.items('search_engine'))

# create engine

search_engine = SearchEngine(search_config)

TENDER_INDEX_KEYS = ['tenders', 'ocds']
PLAN_INDEX_KEYS = ['plans']
ORGS_INDEX_KEYS = ['orgs']


def rename_index_names(config, index_list):
    for i, name in enumerate(index_list):
        rename_key = 'rename_' + name
        if rename_key in config:
            index_list[i] = config[rename_key]
    return index_list

rename_index_names(search_config, TENDER_INDEX_KEYS)
rename_index_names(search_config, PLAN_INDEX_KEYS)
rename_index_names(search_config, ORGS_INDEX_KEYS)
search_server.logger.info("Start with indexes %s %s %s",
    str(TENDER_INDEX_KEYS), str(PLAN_INDEX_KEYS), str(ORGS_INDEX_KEYS))


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
    double = bool(field == 'value.amount')
    body = []
    for q in query:
        if q.find('-') < 0:
            if double:
                q = float(q)
                res = {"range": {field: {"gte": q}}}
            else:
                res = prefix_query([q], field)
            body.append(res)
        else:
            beg, end = q.split('-', 1)
            if double:
                beg, end = float(beg), float(end)
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
    'plan_cpv': 'classification.id',
    'plan_dkpp': 'additionalClassifications.id',
}
match_map = {
    'tid': 'tenderID',
    'pid': 'planID',
    'edrpou': 'procuringEntity.identifier.id',
    'procedure': 'procurementMethod',
    'proc_type': 'procurementMethodType',
    'tender_procedure': 'tender.procurementMethod',
    'tender_proc_type': 'tender.procurementMethodType',
    'plan_procedure': 'tender.procurementMethod',
    'plan_proc_type': 'tender.procurementMethodType',
    'status': 'status',
}
range_map = {
    'region': 'procuringEntity.address.postalCode',
    'value': 'value.amount',
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
    'plan_tender_start':  ('gte', 'tender.tenderPeriod.startDate'),
    'plan_tender_end':    ('lte', 'tender.tenderPeriod.startDate'),
}
ftext_map = {
    'query': '_all',
}


def prepare_search_body(args):
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
        return None
    elif len(body) == 1:
        body = {"query": body[0]}
    else:
        body = {"query": {"bool": {"must": body}}}

    body["sort"] = {"dateModified": {"order": "desc"}}
    return body


@search_server.route('/tenders')
def search_tenders():
    args = request.args
    body = prepare_search_body(args)
    if not body:
        return jsonify({"error": "empty query"})
    start = int(args.get('start') or 0)
    # limit = int(args.get('limit') or 10)
    # limit = min(max(1, limit), 100)
    res = search_engine.search(body, start,
        index_keys=TENDER_INDEX_KEYS)
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/plans')
def search_plans():
    args = request.args
    body = prepare_search_body(args)
    start = int(args.get('start') or 0)
    limit = int(args.get('limit') or 10)
    limit = min(max(1, limit), 100)
    res = search_engine.search(body, start, limit,
        index_keys=PLAN_INDEX_KEYS)
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/orgsuggest')
def orgsuggest():
    # excact search
    edrpou = request.args.get('edrpou', '')
    if edrpou and len(edrpou) < 10:
        body = {
            "size": 1,
            "query": {
                "match": {"edrpou": edrpou}
            }
        }
        res = search_engine.search(body, index_keys=ORGS_INDEX_KEYS)
        return jsonify(res)
    # fulltext search
    query = request.args.get('query', '')
    if not query or len(query) > 50:
        return jsonify({"error": "bad query"})
    fuzziness = 0
    if len(query) > 4:
        fuzziness = 1
    _all = {
        "query": query,
        "operator": "and",
        "fuzziness": fuzziness
    }
    body = {
        "size": 5,
        "query": {
            "match": {"_all": _all}
        }
    }
    body["sort"] = {"rank": {"order": "desc"}}
    res = search_engine.search(body, index_keys=ORGS_INDEX_KEYS)
    if not res.get('items'):
        _all["fuzziness"] += 1
        res = search_engine.search(body, index_keys=ORGS_INDEX_KEYS)
    return jsonify(res)


@search_server.route('/heartbeat')
def heartbeat():
    data = {'heartbeat': search_engine.master_heartbeat()}
    key = request.args.get('key', None)
    if key == search_server.secret_key:
        data['index_names'] = search_engine.index_names_dict()
    elif key:
        abort(403)
    return jsonify(data)


def make_app(global_conf, **kwargs):
    class config:
        pass
    for key, value in kwargs.items():
        setattr(config, key.upper(), value)
    search_server.config.from_object(config)
    return search_server


def main():
    search_server.debug = True
    search_server.logger.info("Start in debug mode with secret_key='%s'",
        search_server.secret_key)
    return search_server.run(host='0.0.0.0')
