# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import sys
import simplejson as json
from ConfigParser import ConfigParser
from flask import Flask, request, jsonify, abort
from time import time

from openprocurement.search.version import __version__
from openprocurement.search.index.asset import AssetIndex
from openprocurement.search.index.dgf_lot import DgfLotIndex
from openprocurement.search.index.auction import AuctionIndex, AuctionIndex2
from openprocurement.search.index.tender import TenderIndex
from openprocurement.search.index.ocds import OcdsIndex
from openprocurement.search.index.plan import PlanIndex
from openprocurement.search.index.orgs import OrgsIndex

from openprocurement.search.engine import SearchEngine
from openprocurement.search.utils import decode_bool_values

# Flask config

JSON_AS_ASCII = False
MAX_SEARCH_ARGS = 12
NAME = 'noname'

# create Flask app

search_server = Flask(__name__)
search_server.config.from_object(__name__)

# load config

config_parser = ConfigParser()
for arg in sys.argv:
    if arg.endswith('.ini'):
        config_parser.read(arg)
search_config = dict(config_parser.items('search_engine'))
search_config = decode_bool_values(search_config)

# create engine

search_engine = SearchEngine(search_config, role='search')
search_engine.init_search_map({
    'lots': [DgfLotIndex],
    'assets': [AssetIndex],
    'auctions': [AuctionIndex],
    'auctions2': [AuctionIndex2],
    'auctions3': [AuctionIndex, AuctionIndex2],
    'tenders': [TenderIndex, OcdsIndex],
    'plans': [PlanIndex],
    'orgs': [OrgsIndex],
})

# query fileds map

prefix_map = {
    'asid_like': 'assetID',
    'aid_like': 'auctionID',
    'dgf_like': 'dgfID',
    'lid_like': 'lotID',
    'tid_like': 'tenderID',
    'pid_like': 'planID',
    'cav_like': 'items.classification.id',
    'cpv_like': 'items.classification.id',
    'dkpp_like': 'items.additionalClassifications.id',
    'cpvs_like': 'items.additionalClassifications.id',
    'plan_cpv_like': 'classification.id',
    'plan_dkpp_like': 'additionalClassifications.id',
    'asset_cav_like': 'classification.id',
    'asset_cpvs_like': 'additionalClassifications.id',
}
match_map = {
    'id': 'id',
    'asid': 'assetID',
    'aid': 'auctionID',
    'dgf': 'dgfID',
    'lid': 'lotID',
    'tid': 'tenderID',
    'pid': 'planID',
    'cav': 'items.classification.id',
    'cpv': 'items.classification.id',
    'dkpp': 'items.additionalClassifications.id',
    'cpvs': 'items.additionalClassifications.id',
    'asset_cav': 'classification.id',
    'asset_cpvs': 'additionalClassifications.id',
    'plan_cpv': 'classification.id',
    'plan_dkpp': 'additionalClassifications.id',
    'asset_edrpou': 'assetCustodian.identifier.id',
    'lot_edrpou': 'lotCustodian.identifier.id',
    'edrpou': 'procuringEntity.identifier.id',
    'award_edrpou': 'awards.suppliers.identifier.id',
    'supplier_edrpou': 'awards.suppliers.identifier.id',
    'supplier_active': 'awards.suppliers.identifier.active',
    'tenderer_edrpou': 'bids.tenderers.identifier.id',
    'contract_edrpou': 'contracts.suppliers.identifier.id',
    'contract_active': 'contracts.suppliers.identifier.active',
    'complaint_edrpou': 'complaints.author.identifier.id',
    'procedure': 'procurementMethod',
    'proc_type': 'procurementMethodType',
    'asset_type': 'assetType',
    'lot_type': 'lotType',
    'tender_procedure': 'tender.procurementMethod',
    'tender_proc_type': 'tender.procurementMethodType',
    'plan_procedure': 'tender.procurementMethod',
    'plan_proc_type': 'tender.procurementMethodType',
    'award_criteria': 'awardCriteria',
    'unit_code': 'unit.code',
    'status': 'status',
}
match_multi_map = {
    'sp_make': ('schema_properties.properties.make', 'items.schema_properties.properties.make'),
    'sp_model': ('schema_properties.properties.model', 'items.schema_properties.properties.model'),
    'sp_cadastralNumber':  ('schema_properties.properties.cadastralNumber', 'items.schema_properties.properties.cadastralNumber'),
}
range_multi_map = {
    'sp_productionDate': ('schema_properties.properties.productionDate', 'items.schema_properties.properties.productionDate'),
    'sp_area': ('schema_properties.properties.area', 'items.schema_properties.properties.area'),
    'sp_totalArea': ('schema_properties.properties.totalArea', 'items.schema_properties.properties.totalArea'),
    'sp_livingArea': ('schema_properties.properties.livingArea', 'items.schema_properties.properties.livingArea'),
}
range_map = {
    'region': 'procuringEntity.address.postalCode',
    'address_region': 'address.postalCode',
    'asset_region': 'assetCustodian.address.postalCode',
    'lot_region': 'lotCustodian.address.postalCode',
    'item_region': 'items.address.postalCode',
    'item_square': 'items.quantity_MTK',
    'value': 'value.amount',
    'budget': 'budget.amount',
    'quantity': 'quantity',
}
dates_map = {
    # auctions may not set endDate, use only startDate
    'auction_start': ('gte', 'auctionPeriod.startDate'),
    'auction_end':   ('lt',  'auctionPeriod.startDate'),
    # use custom filed activeDate (see source.patch_tender)
    'award_start':   ('gte', 'awards.activeDate'),
    'award_end':     ('lt',  'awards.activeDate'),
    # use custom field activeDate (see source.patch_tender)
    'contract_start': ('gte', 'contracts.activeDate'),
    'contract_end':   ('lt',  'contracts.activeDate'),
    # tender.date
    'date_start':    ('gte', 'date'),
    'date_end':      ('lt',  'date'),
    # tender.dateModified
    'datemod_start': ('gte', 'dateModified'),
    'datemod_end':   ('lt',  'dateModified'),
    # plan.datePublished
    'datepub_start': ('gte', 'datePublished'),
    'datepub_end':   ('lt',  'datePublished'),
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
    'award_edrpou_all': 'awards.suppliers.identifier.id',
    'supplier_edrpou_all': 'awards.suppliers.identifier.id',
    'bid_edrpou_all': 'bids.tenderers.identifier.id',
    'tenderer_edrpou_all': 'bids.tenderers.identifier.id',
    'contract_edrpou_all': 'contracts.suppliers.identifier.id',
    'complaint_edrpou_all': 'complaints.author.identifier.id',
}
sorting_map = {
    'date': 'date',
    'dateModified': 'dateModified',
    'datePublished': 'datePublished',
    'value': 'value.amount',
    'budget': 'budget.amount',
}
auction_map_fields = [
    'address',
    'auctionID',
    'auctionPeriod',
    'description',
    'id',
    'items',
    'procuringEntity',
    'title',
    'value',
]
short_auction_map_fields = [
    'id',
    'items.id'
]

# global search map for plugins

search_engine.init_search_plugins({
    'prefix_map': prefix_map,
    'match_map': match_map,
    'match_multi_map': match_multi_map,
    'range_multi_map': range_multi_map,
    'range_map': range_map,
    'dates_map': range_map,
    'fulltext_map': fulltext_map,
    'sorting_map': sorting_map,
})

# convert auction response to map items

def convert_auction_map_items(in_items, short=False):
    out_items = list()

    for auction in in_items:
        if 'items' not in auction:
            continue
        for item in auction['items']:
            if short:
                map_item = {
                    'id': '{}_{}'.format(auction['id'], item['id']),
                }
            else:
                map_item = {
                    'id': '{}_{}'.format(auction['id'], item['id']),
                    'auctionID': auction.get('auctionID'),
                    'title': auction.get('title', ''),
                    'description': item.get('description', '') or auction.get('description', ''),
                    'auctionPeriodStartDate': auction.get('auctionPeriod', {}).get('startDate', None),
                    'procuringEntityName': auction.get('procuringEntity', {}).get('name', ''),
                    'address': item.get('address', None) or auction.get('address', None),
                    'value': item.get('value', None) or auction.get('value', None),
                }
            out_items.append(map_item)

    return out_items


# build query helper functions


def match_query(query, field, type_=None, operator=None, analyzer=None, force_lower=False):
    qtext = " ".join(query)
    if force_lower:
        qtext = qtext.lower()
    query = {"query": qtext}
    if operator and qtext.find(" ") >= 0:
        query["operator"] = operator
    if analyzer:
        query["analyzer"] = analyzer
    if type_:
        query["type"] = type_
    return {"match": {field: query}}


def match_multi(query, fields, force_lower=False):
    body = []
    for q in query:
        if force_lower:
            q = q.lower()
        for f in fields:
            body.append({"match": {f: q}})
    return {"bool": {"should": body}}


def prefix_query(query, field, force_lower=False):
    body = []
    for q in query:
        if force_lower:
            q = q.lower()
        sq = {field: {"prefix": q}}
        body.append({"prefix": sq})
    if len(body) == 1:
        return body[0]
    return {"bool": {"should": body}}


def range_query(query, field, force_float=False):
    body = []
    for q in query:
        if q.find('-') < 0:
            if force_float:
                q = float(q)
                res = {"range": {field: {"gte": q}}}
            else:
                res = prefix_query([q], field)
            body.append(res)
        else:
            beg, end = q.split('-', 1)
            if force_float:
                beg, end = float(beg), float(end)
            elif 'postalCode' in field:  # FIXME
                end += '999'
            body.append({"range": {
                field: {"gte": beg, "lte": end}
            }})
    if len(body) == 1:
        return body[0]
    return {"bool": {"should": body}}


def range_multi(query, fields, force_float=False):
    body = []
    for f in fields:
        rq = range_query(query, f, force_float)
        if "bool" in rq:
            rq = rq["bool"]["should"]
        body.append(rq)
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
        for rk, rv in q["range"].items():
            if rk == key:
                rv[op] = query
                return
    match = dates_query(query, args)
    body.append(match)


# build query body


def prepare_search_body(args, default_sort='dateModified', source_fields=None):
    force_lower = int(search_config.get('force_lower', 1))
    body = list()

    if len(args) > MAX_SEARCH_ARGS:
        raise ValueError('Too many argumets')

    # hierarchical classifiers
    for key in prefix_map.keys():
        if not args.get(key):
            continue
        field = prefix_map[key]
        query = args.getlist(key)
        match = prefix_query(query, field,
            force_lower=force_lower)
        body.append(match)

    # ID's and states
    for key in match_map.keys():
        if not args.get(key):
            continue
        field = match_map[key]
        query = args.getlist(key)
        match = match_query(query, field,
            operator='or',
            analyzer='whitespace',
            force_lower=force_lower)
        body.append(match)

    # multi fields match
    for key in match_multi_map.keys():
        if not args.get(key):
            continue
        fields = match_multi_map[key]
        query = args.getlist(key)
        match = match_multi(query, fields, force_lower)
        body.append(match)

    # multi fields range query
    for key in range_multi_map.keys():
        if not args.get(key):
            continue
        fields = range_multi_map[key]
        query = args.getlist(key)
        match = range_multi(query, fields, True)
        body.append(match)

    # range values ie postal code
    for key in range_map.keys():
        if not args.get(key):
            continue
        field = range_map[key]
        query = args.getlist(key)
        float_field = 'postalCode' not in field
        match = range_query(query, field, float_field)
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
        body = {'query': {'match_all': {}}}
    elif len(body) == 1:
        body = {'query': body[0]}
    else:
        body = {'query': {'bool': {'must': body}}}

    sort = args.get('sort') or default_sort
    order = args.get('order')

    if order != 'asc':
        order = 'desc'

    if sort in sorting_map:
        body['sort'] = {sorting_map[sort]: {'order': order}}
    elif sort == '_score' and args.get('query'):
        body.pop('sort')
    else:
        body['sort'] = {default_sort: {'order': 'desc'}}

    if source_fields is not None:
        body['_source'] = source_fields

    return body


@search_server.route('/tenders')
def search_tenders():
    try:
        args = request.args
        body = prepare_search_body(args, default_sort='date')
        start = int(args.get('start') or 0)
        limit = int(args.get('limit') or 10)
        limit = min(max(1, limit), 100)
        res = search_engine.search(body, start, limit, index_set='tenders')
    except Exception as e:
        search_server.logger.exception("Error in tenders {}".format(e))
        res = {"error": "{}: {}".format(type(e).__name__, e)}
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/plans')
def search_plans():
    try:
        args = request.args
        body = prepare_search_body(args, default_sort='datePublished')
        start = int(args.get('start') or 0)
        limit = int(args.get('limit') or 10)
        limit = min(max(1, limit), 100)
        res = search_engine.search(body, start, limit, index_set='plans')
    except Exception as e:
        search_server.logger.exception("Error in plans {}".format(e))
        res = {"error": "{}: {}".format(type(e).__name__, e)}
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/auctions')
def search_auctions():
    try:
        args = request.args
        body = prepare_search_body(args, default_sort='date')
        start = int(args.get('start') or 0)
        limit = int(args.get('limit') or 10)
        limit = min(max(1, limit), 100)
        index_key = int(args.get('index') or 1)
        index_set = ['auctions', 'auctions2', 'auctions3'][index_key - 1]
        res = search_engine.search(body, start, limit, index_set=index_set)
    except Exception as e:
        search_server.logger.exception("Error in auctions {}".format(e))
        res = {"error": "{}: {}".format(type(e).__name__, e)}
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/auctions.map')
def search_auctions_map():
    try:
        args = request.args
        short = int(args.get('short') or 0)
        fields = short_auction_map_fields if short else auction_map_fields
        body = prepare_search_body(args, default_sort='date', source_fields=fields)
        start = int(args.get('start') or 0)
        limit = int(args.get('limit') or 100)
        limit = min(max(1, limit), 1000)
        index_key = int(args.get('index') or 1)
        index_set = ['auctions', 'auctions2', 'auctions3'][index_key - 1]
        res = search_engine.search(body, start, limit, index_set=index_set)
        if res and 'items' in res:
            items = res.pop('items')
            res['count'] = len(items)
            res['items'] = convert_auction_map_items(items, short)
    except Exception as e:
        search_server.logger.exception("Error in auctions.map {}".format(e))
        res = {"error": "{}: {}".format(type(e).__name__, e)}
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/assets')
def search_assets():
    try:
        args = request.args
        body = prepare_search_body(args, default_sort='date')
        start = int(args.get('start') or 0)
        limit = int(args.get('limit') or 10)
        limit = min(max(1, limit), 100)
        res = search_engine.search(body, start, limit, index_set='assets')
    except Exception as e:
        search_server.logger.exception("Error in assets {}".format(e))
        res = {"error": "{}: {}".format(type(e).__name__, e)}
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/lots')
def search_lots():
    try:
        args = request.args
        body = prepare_search_body(args, default_sort='date')
        start = int(args.get('start') or 0)
        limit = int(args.get('limit') or 10)
        limit = min(max(1, limit), 100)
        res = search_engine.search(body, start, limit, index_set='lots')
    except Exception as e:
        search_server.logger.exception("Error in lots {}".format(e))
        res = {"error": "{}: {}".format(type(e).__name__, e)}
    if search_server.debug:
        res['body'] = body
    return jsonify(res)


@search_server.route('/orgsuggest')
def orgsuggest():
    # excact search
    if request.args.get('edrpou', ''):
        edrpou = request.args.getlist('edrpou')
        limit = len(edrpou)
        if limit > 100:
            return jsonify({"error": "too many edrpou"})
        body = {"query": {"terms": {"edrpou": edrpou}}}
        res = search_engine.search(body, limit=limit, index_set='orgs')
        return jsonify(res)
    # generate static top-orgs json
    toporgs = request.args.get('toporgs', '')
    if toporgs and int(toporgs) < 1001:
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
    tenderer = request.args.get('tenderer', None)
    if not query or len(query) > 50:
        return jsonify({"error": "bad query"})
    _all = {
        "query": query,
        "operator": "and"
    }
    body = {
        "query": {"match": {"_all": _all}},
        "sort": {"rank": {"order": "desc"}},
    }
    if len(query) > 8:
        _all["fuzziness"] = 1
    if tenderer is not None:
        tenderer = int(tenderer or 0)
        body["query"] = {
            "filtered": {
                "query": body["query"],
                "filter": {"term": {"tenderer": tenderer}}
            }
        }
    limit = int(request.args.get('limit') or 10)
    if limit < 1 or limit > 100:
        return jsonify({"error": "bad limit"})
    res = search_engine.search(body, limit=limit, index_set='orgs')
    if not res.get('items') and "filtered" in body["query"]:
        body["query"] = body["query"]["filtered"]["query"]
    if not res.get('items'):
        _all["fuzziness"] = _all.get("fuzziness", 0) + 1
        res = search_engine.search(body, limit=limit, index_set='orgs')
    if search_server.debug and res:
        res["body"] = body
    return jsonify(res)


@search_server.route('/heartbeat', methods=['GET', 'HEAD', 'POST'])
def heartbeat():
    data = {
        'name': search_server.config.get('NAME'),
        'uptime': int(time() - search_server.config.get('START_TIME', 0)),
        'heartbeat': int(search_engine.master_heartbeat() or 0),
        'version': __version__
    }
    key = request.values.get('key', None)
    if key and key == search_server.secret_key:
        data['index_names'] = search_engine.index_names_dict()
        data['index_stats'] = search_engine.index_docs_count()
        if request.values.get('config', ''):
            data['search_config'] = search_config.copy()
            for k in data['search_config']:
                if '_passw' in k or '_key' in k:
                    data['search_config'][k] = '***'
        if search_engine.search_plugins:
            data['search_plugins'] = [repr(p) for p in search_engine.search_plugins]
        if search_server.debug:
            data['debug'] = True
    elif key:
        abort(403)
    res = jsonify(data)
    if request.values.get('pretty', ''):
        res.set_data(json.dumps(data, sort_keys=True, indent=4))
    return res


@search_server.route('/', methods=['GET', 'HEAD', 'POST'])
def root():
    return heartbeat()


def make_app(global_conf, **kwargs):
    class config:
        pass
    for key, value in kwargs.items():
        setattr(config, key.upper(), value)
    search_server.config.from_object(config)
    search_server.config['START_TIME'] = time()
    return search_server


def main():
    search_server.debug = True
    search_server.logger.info("Start in debug mode with secret_key='%s'",
        search_server.secret_key)
    return search_server.run(host='0.0.0.0')
