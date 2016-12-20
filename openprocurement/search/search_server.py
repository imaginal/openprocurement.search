# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import re
import sys
import simplejson as json
from ConfigParser import ConfigParser
from flask import Flask, request, jsonify, abort

from openprocurement.search.index.auction import AuctionIndex
from openprocurement.search.index.tender import TenderIndex
from openprocurement.search.index.ocds import OcdsIndex
from openprocurement.search.index.plan import PlanIndex
from openprocurement.search.index.orgs import OrgsIndex

from openprocurement.search.engine import SearchEngine

# Flas config

JSONIFY_PRETTYPRINT_REGULAR = False

# create Flask app

search_server = Flask(__name__)
search_server.config.from_object(__name__)

# greetings

search_server.logger.info("Starting ProZorro search_server v%s", __version__)
search_server.logger.info("Copyright (c) 2016 Volodymyr Flonts")

# load config

config_parser = ConfigParser()
for arg in sys.argv:
    if arg.endswith('.ini'):
        config_parser.read(arg)
search_config = dict(config_parser.items('search_engine'))

# create engine

search_engine = SearchEngine(search_config)
search_engine.init_search_map({
    'auctions': [AuctionIndex],
    'tenders': [TenderIndex, OcdsIndex],
    'plans': [PlanIndex],
    'orgs': [OrgsIndex],
})

# query fileds map

prefix_map = {
    'aid_like': 'auctionID',
    'dgf_like': 'dgfID',
    'tid_like': 'tenderID',
    'pid_like': 'planID',
    'cpv_like': 'items.classification.id',
    'dkpp_like': 'items.additionalClassifications.id',
    'plan_cpv_like': 'classification.id',
    'plan_dkpp_like': 'additionalClassifications.id',
}
match_map = {
    'id': 'id',
    'aid': 'auctionID',
    'dgf': 'dgfID',
    'tid': 'tenderID',
    'pid': 'planID',
    'cpv': 'items.classification.id',
    'dkpp': 'items.additionalClassifications.id',
    'plan_cpv': 'classification.id',
    'plan_dkpp': 'additionalClassifications.id',
    'edrpou': 'procuringEntity.identifier.id',
    'procedure': 'procurementMethod',
    'proc_type': 'procurementMethodType',
    'tender_procedure': 'tender.procurementMethod',
    'tender_proc_type': 'tender.procurementMethodType',
    'plan_procedure': 'tender.procurementMethod',
    'plan_proc_type': 'tender.procurementMethodType',
    'award_criteria': 'awardCriteria',
    'status': 'status',
}
range_map = {
    'region': 'procuringEntity.address.postalCode',
    'value': 'value.amount',
}
dates_map = {
    # auctions may not set endDate, use only startDate
    'auction_start': ('gte', 'auctionPeriod.startDate'),
    'auction_end':   ('lt',  'auctionPeriod.startDate'),
    # use custom filed activeDate (see source.patch_tender)
    'award_start':   ('gte', 'awards.activeDate'),
    'award_end':     ('lt',  'awards.activeDate'),
    # use custom field activeDate (see source.patch_tender)
    'contract_start':('gte', 'contracts.activeDate'),
    'contract_end':  ('lt',  'contracts.activeDate'),
    # for enquiry use only startDate
    'enquiry_start': ('gte', 'enquiryPeriod.startDate'),
    'enquiry_end':   ('lt',  'enquiryPeriod.startDate'),
    # tender period
    'tender_start':  ('gte', 'tenderPeriod.endDate'),
    'tender_end':    ('lt',  'tenderPeriod.startDate'),
    # plans don't set tenderPeriod.endDate, use only startDate
    'plan_tender_start':  ('gte', 'tender.tenderPeriod.startDate'),
    'plan_tender_end':    ('lt',  'tender.tenderPeriod.startDate'),
}
fulltext_map = {
    'query': '_all',
}

# build query helper functions

def match_query(query, field, type_=None, operator=None, analyzer=None):
    qtext = " ".join(query)
    query = {"query": qtext}
    if operator and qtext.find(" ") >= 0:
        query["operator"] = operator
    if analyzer:
        query["analyzer"] = analyzer
    if type_:
        query["type"] = type_
    return {"match": {field: query}}


def prefix_query(query, field):
    body = []
    for q in query:
        query = {field: {"prefix": q}}
        body.append({"prefix": query})
    if len(body) == 1:
        return body[0]
    return {"bool": {"should": body}}


def range_query(query, field):
    is_double = field in ('value.amount',)
    body = []
    for q in query:
        if q.find('-') < 0:
            if is_double:
                q = float(q)
                res = {"range": {field: {"gte": q}}}
            else:
                res = prefix_query([q], field)
            body.append(res)
        else:
            beg, end = q.split('-', 1)
            if is_double:
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


def append_dates_query(body, query, args):
    op, key = args
    for q in body:
        if "range" not in q:
            continue
        for rk,rv in q["range"].items():
            if rk == key:
                rv[op] = query
                return
    match = dates_query(query, args)
    body.append(match)

# build query body

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
        append_dates_query(body, query, field)

    # full-text search
    for key in fulltext_map.keys():
        if not args.get(key):
            continue
        field = fulltext_map[key]
        query = args.getlist(key)
        match = match_query(query, field,
            operator='and')
        body.append(match)

    if not body:
        return None
    elif len(body) == 1:
        body = {'query': body[0]}
    else:
        body = {'query': {'bool': {'must': body}}}

    sort = args.get('sort', 'date')

    if sort == 'rank' and args.get('query'):
        body.pop('sort') # default fulltext sort
    elif sort == 'value':
        body['sort'] = {'value.amount': {'order': 'desc'}}
    else:
        body['sort'] = {'dateModified': {'order': 'desc'}}

    return body


@search_server.route('/tenders')
def search_tenders():
    args = request.args
    body = prepare_search_body(args)
    if not body:
        return jsonify({"error": "empty query"})
    start = int(args.get('start') or 0)
    limit = int(args.get('limit') or 10)
    limit = min(max(1, limit), 100)
    res = search_engine.search(body, start, limit, index_set='tenders')
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
    res = search_engine.search(body, start, limit, index_set='plans')
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/auctions')
def search_auctions():
    args = request.args
    body = prepare_search_body(args)
    start = int(args.get('start') or 0)
    limit = int(args.get('limit') or 10)
    limit = min(max(1, limit), 100)
    res = search_engine.search(body, start, limit, index_set='auctions')
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/orgsuggest')
def orgsuggest():
    # excact search
    edrpou = request.args.get('edrpou', '')
    if edrpou and len(edrpou) < 10:
        body = {
            "query": {"match": {"edrpou": edrpou}},
        }
        res = search_engine.search(body, limit=1, index_set='orgs')
        return jsonify(res)
    # generate static top-orgs json
    toporgs = request.args.get('toporgs', '')
    if toporgs and int(toporgs) < 1000:
        body = {
            "query": {"match_all": {}},
            "sort": {"rank": {"order": "desc"}},
        }
        limit = int(toporgs)
        res = search_engine.search(body, limit=limit, index_set='orgs')
        if request.args.get('plain', ''):
            items = dict()
            for i in res['items']:
                edrpou = i['edrpou']
                items[edrpou] = i['name']
            return jsonify(items)
        return jsonify(res)
    # fulltext search
    query = request.args.get('query', '')
    if not query or len(query) > 50:
        return jsonify({"error": "bad query"})
    fuzziness = 0
    if len(query) > 8:
        fuzziness = 1
    _all = {
        "query": query,
        "operator": "and",
        "fuzziness": fuzziness
    }
    body = {
        "query": {"match": {"_all": _all}},
        "sort": {"rank": {"order": "desc"}},
    }
    res = search_engine.search(body, limit=5, index_set='orgs')
    if not res.get('items'):
        _all["fuzziness"] += 1
        res = search_engine.search(body, limit=5, index_set='orgs')
    return jsonify(res)


@search_server.route('/heartbeat')
def heartbeat():
    data = {'heartbeat': search_engine.master_heartbeat()}
    key = request.args.get('key', None)
    if key and key == search_server.secret_key:
        data['index_names'] = search_engine.index_names_dict()
        data['index_stats'] = search_engine.index_docs_count()
    elif key:
        abort(403)
    res = jsonify(data)
    if request.args.get('pretty', ''):
        res.set_data(json.dumps(data, sort_keys=True, indent=4))
    return res

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
