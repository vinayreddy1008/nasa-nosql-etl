import pytest
from common.spec import parse_line, reference_aggregate
from app.validation import validate_result


def line(host='a.example',date='01/Jul/1995:00:00:01',req='GET /x HTTP/1.0',
         status=200,byte='5'):
    return f'{host} - - [{date} -0400] "{req}" {status}'+(f' {byte}' if byte is not None else '')


def test_missing_dash_empty_bytes_are_zero():
    for val in ('-', None,'0'):
        p=parse_line(line(byte=val))
        assert p and p['bytes_transferred']==0


def test_bad_request_and_date_are_malformed():
    assert parse_line('not a log') is None
    assert parse_line(line(req='invalid request with too many tokens')) is None
    assert parse_line(line(date='31/Feb/1995:00:00:01')) is None
    assert parse_line(line(date='01/Jul/1995:24:00:01')) is None


def test_valid_path_status_request_and_timezone():
    p=parse_line(line(req='POST /path?k=v HTTP/1.1',status=503,byte='99'))
    assert p['resource_path']=='/path'
    assert p['http_method']=='POST' and p['protocol_version']=='HTTP/1.1'
    assert p['log_date']=='1995-07-01' and p['log_hour']==0
    assert p['timestamp'].endswith(' -0400')


def test_reference_aggregations():
    rows=[line('a',req='GET /a HTTP/1.0',byte='10'),
          line('b',req='GET /a HTTP/1.0',byte='20',status=404),
          line('a',req='GET /b HTTP/1.0',byte='-',status=503),
          line('a',req='GET /a?query=1 HTTP/1.0',byte=None), 'unparseable']
    data=reference_aggregate(rows)
    assert data['malformed_records']==1
    assert data['q2'][0]=={'resource_path':'/a','request_count':3,'total_bytes':30,'distinct_host_count':2}
    assert data['q3'][0]['error_request_count']==2
    assert data['q3'][0]['distinct_error_hosts']==2
    data['batches']=[{'batch_id':1,'source_file':'x','input_count':2},
                     {'batch_id':2,'source_file':'x','input_count':2},
                     {'batch_id':3,'source_file':'x','input_count':1}]
    validate_result(data)


def test_error_window_inclusive_and_not_other_codes():
    rows=[line('a',status=400),line('b',status=599),
          line('c',status=399),line('d',status=600)]
    d=reference_aggregate(rows)
    q=d['q3'][0]
    assert q['error_request_count']==2 and q['total_request_count']==4
    assert q['error_rate']==0.5 and q['distinct_error_hosts']==2


def test_query2_top20_and_stable_tiebreak():
    rows=[line('x',req=f'GET /p{i:02d} HTTP/1.0') for i in range(25)]
    got=reference_aggregate(rows)['q2']
    assert len(got)==20
    assert [r['resource_path'] for r in got]==[f'/p{i:02d}' for i in range(20)]


def test_date_hour_groups_do_not_utc_shift():
    rows=[line(date='31/Jul/1995:23:59:59',status=404),
          line(date='01/Aug/1995:00:00:00',status=404)]
    d=reference_aggregate(rows)
    assert [(r['log_date'],r['log_hour']) for r in d['q3']]==[
        ('1995-07-31',23),('1995-08-01',0)]
