"""Real MongoDB aggregation implementation; Python never parses the raw records.

Raw logs are inserted unmodified with transport metadata. All parsing, field
extraction, malformed filtering, grouping, distinct counting and top-20 work
runs inside MongoDB aggregation pipelines.
"""
from __future__ import annotations
from pathlib import Path
import os
import re
from pymongo import MongoClient
from common.spec import LOG_PATTERN, REQUEST_PATTERN, MONTHS


def _cap(field, ix):
    return {'$arrayElemAt': [f'${field}.captures', ix]}


def _num(expr):
    return {'$convert': {'input': expr, 'to': 'int', 'onError': None, 'onNull': None}}


def _cat(*args):
    return {'$concat': list(args)}


def _parsed_stages():
    """Only Mongo expressions transform raw NASA records."""
    mon_expr = {'$indexOfArray': [MONTHS, '$month_text']}
    date_expr = _cat('$year_text', '-',
                     {'$cond': [{'$lt':['$month_index', 9]},
                                _cat('0',{'$toString':{'$add':['$month_index',1]}}),
                                {'$toString':{'$add':['$month_index',1]}}]},
                     '-', '$day_text')
    bytes_expr = {'$cond': [{'$in': ['$byte_text', [None, '-']]}, 0, {'$toLong': '$byte_text'}]}
    return [
        {'$project': {'batch_id':1, 'source_file':1,
                      'm':{'$regexFind':{'input':'$line','regex':LOG_PATTERN}}}},
        {'$set': {'r': {'$regexFind': {
            'input': {'$ifNull': [_cap('m', 8), '']}, 'regex': REQUEST_PATTERN}}}},
        {'$set': {
            'host': _cap('m', 0),'day_text': _cap('m',1),'month_text': _cap('m',2),
            'year_text': _cap('m',3),'hour_text': _cap('m',4),
            'minute_text': _cap('m',5),'second_text': _cap('m',6),
            'timezone_text': _cap('m',7),'request_text': _cap('m',8),
            'status_text': _cap('m',9),'byte_text': _cap('m',10),
            'http_method': _cap('r',0),'target':_cap('r',1),
            'protocol_version':_cap('r',2),
        }},
        {'$set': {
            'month_index': mon_expr,
            'day_number':_num('$day_text'),
            'hour_number':_num('$hour_text'),
            'minute_number':_num('$minute_text'),
            'second_number':_num('$second_text'),
            'status_code':_num('$status_text'),
        }},
        {'$set': {'actual_date': {'$dateFromString': {
            'dateString': date_expr, 'format': '%Y-%m-%d', 'onError': None, 'onNull': None}}}},
        {'$set': {'valid': {'$and': [
            {'$ne': ['$m', None]}, {'$ne': ['$r', None]}, {'$ne': ['$actual_date', None]},
            {'$gte': ['$month_index',0]},
            {'$gte': ['$day_number',1]}, {'$lte':['$day_number',31]},
            {'$gte': ['$hour_number',0]}, {'$lte':['$hour_number',23]},
            {'$gte': ['$minute_number',0]}, {'$lte':['$minute_number',59]},
            {'$gte': ['$second_number',0]}, {'$lte':['$second_number',59]},
        ]}}},
        {'$set': {
            'timestamp': {'$cond': ['$valid',_cat('$day_text','/','$month_text','/','$year_text',
                                   ':','$hour_text',':','$minute_text',':','$second_text',' ','$timezone_text'),None]},
            'log_date': {'$cond':['$valid',date_expr,None]},
            'log_hour': {'$cond':['$valid','$hour_number',None]},
            'resource_path': {'$cond':['$valid',{'$arrayElemAt':[{'$split':['$target','?']},0]},None]},
            'bytes_transferred': {'$cond':['$valid',bytes_expr,0]},
        }},
        {'$project': {'_id':0,'batch_id':1,'source_file':1,'valid':1,'host':1,
          'timestamp':1,'log_date':1,'log_hour':1,'http_method':1,
          'resource_path':1,'protocol_version':1,'status_code':1,'bytes_transferred':1}},
    ]


def execute(files: list[Path], batch_size: int, run_id: str) -> dict:
    uri = os.environ.get('MONGO_URI','mongodb://localhost:27018')
    client = MongoClient(uri, serverSelectionTimeoutMS=12000)
    client.admin.command('ping')
    db = client['nasa_phase1']
    # A separate collection for each run prevents stale results and cross-run contamination.
    suffix = re.sub('[^a-zA-Z0-9_]', '_', run_id)
    raw = db['raw_' + suffix]
    clean = db['parsed_' + suffix]
    batches=[]
    total=0
    try:
        batch_id=0
        for file in files:
            pending=[]
            with file.open('r', encoding='latin-1') as f:
                for original_line in f:
                    pending.append({'line':original_line.rstrip('\r\n'),
                                    'source_file':file.name,'batch_id':batch_id+1})
                    if len(pending) == batch_size:
                        batch_id += 1
                        raw.insert_many(pending, ordered=False)
                        batches.append(dict(batch_id=batch_id,source_file=file.name,input_count=len(pending)))
                        total += len(pending)
                        pending=[]
            if pending:
                batch_id+=1
                raw.insert_many(pending, ordered=False)
                batches.append(dict(batch_id=batch_id,source_file=file.name,input_count=len(pending)))
                total+=len(pending)
        if total == 0:
            raise ValueError('No input lines; refusing to report an empty execution')
        raw.aggregate(_parsed_stages()+[{'$out':clean.name}], allowDiskUse=True, maxTimeMS=0)
        malformed=clean.count_documents({'valid':False})
        valid=[{'$match':{'valid':True}}]
        q1 = list(clean.aggregate(valid + [
            {'$group':{'_id':{'log_date':'$log_date','status_code':'$status_code'},
                       'request_count':{'$sum':1},'total_bytes':{'$sum':'$bytes_transferred'}}},
            {'$project':{'_id':0,'log_date':'$_id.log_date','status_code':'$_id.status_code',
                         'request_count':1,'total_bytes':1}},
            {'$sort':{'log_date':1,'status_code':1}}],allowDiskUse=True))
        q2 = list(clean.aggregate(valid + [
            {'$group':{'_id':'$resource_path','request_count':{'$sum':1},
                       'total_bytes':{'$sum':'$bytes_transferred'},'hosts':{'$addToSet':'$host'}}},
            {'$project':{'_id':0,'resource_path':'$_id','request_count':1,
                         'total_bytes':1,'distinct_host_count':{'$size':'$hosts'}}},
            {'$sort':{'request_count':-1,'resource_path':1}}, {'$limit':20}],allowDiskUse=True))
        is_error={'$and':[{'$gte':['$status_code',400]}, {'$lte':['$status_code',599]}]}
        q3 = list(clean.aggregate(valid + [
            {'$group':{'_id':{'log_date':'$log_date','log_hour':'$log_hour'},
                       'error_request_count':{'$sum':{'$cond':[is_error,1,0]}},
                       'total_request_count':{'$sum':1},
                       'error_hosts':{'$addToSet':{'$cond':[is_error,'$host',None]}}}},
            {'$project':{'_id':0,'log_date':'$_id.log_date','log_hour':'$_id.log_hour',
                         'error_request_count':1,'total_request_count':1,
                         'error_rate':{'$divide':['$error_request_count','$total_request_count']},
                         'distinct_error_hosts':{'$size':{'$setDifference':['$error_hosts',[None]]}}}},
            {'$sort':{'log_date':1,'log_hour':1}}],allowDiskUse=True))
        return dict(q1=q1,q2=q2,q3=q3,malformed_records=malformed,
                    total_records=total,batches=batches)
    finally:
        # Clean up per-run staging, including after an exception.
        raw.drop()
        clean.drop()
        client.close()
