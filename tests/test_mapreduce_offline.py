"""Execute the actual Hadoop mapper/reducer Python programs without Docker.

This checks algorithmic correctness, not that Hadoop orchestration succeeds.
"""
import os
import subprocess
import sys
from pathlib import Path
from common.spec import reference_aggregate
from tests.test_reference import line

ROOT=Path(__file__).resolve().parents[1]
MR=ROOT/'pipelines/mapreduce'


def script(name, payload):
    run=subprocess.run([sys.executable,str(MR/name)],input=payload,encoding='utf-8',
                       stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=ROOT)
    assert run.returncode==0,run.stderr
    return run.stdout


def test_mapreduce_scripts_against_reference():
    raw='\n'.join([
        line('a',req='GET /a HTTP/1.0',status=200,byte='9'),
        line('b',req='GET /a HTTP/1.0',status=404,byte='-'),
        line('a',req='GET /b HTTP/1.0',status=503,byte=None),
        line('c',req='GET /a?q=1 HTTP/1.0',status=200,byte='11'),
        'broken log entry',
    ])+'\n'
    stage1=script('reducer.py',''.join(sorted(script('mapper.py',raw).splitlines(True))))
    stage2=script('top_reducer.py',''.join(sorted(script('top_mapper.py',stage1).splitlines(True))))
    oracle=reference_aggregate(raw.splitlines())
    q1=[];q2=[];q3=[];bad=0;batch=0
    for s in stage1.splitlines():
        parts=s.split('\t')
        if parts[0]=='Q1':
            _,date,status,n,b=parts
            q1.append(dict(log_date=date,status_code=int(status),request_count=int(n),total_bytes=int(b)))
        elif parts[0]=='Q3':
            _,date,hour,e,n,rate,h=parts
            q3.append(dict(log_date=date,log_hour=int(hour),error_request_count=int(e),
                      total_request_count=int(n),error_rate=float(rate),distinct_error_hosts=int(h)))
        elif parts[0]=='M':bad+=int(parts[-1])
        elif parts[0]=='B':batch+=int(parts[-1])
    for s in stage2.splitlines():
        _,p,n,b,h=s.split('\t')
        q2.append(dict(resource_path=p,request_count=int(n),total_bytes=int(b),distinct_host_count=int(h)))
    assert sorted(q1,key=lambda r:(r['log_date'],r['status_code']))==oracle['q1']
    assert q2==oracle['q2']
    assert q3==oracle['q3']
    assert bad==oracle['malformed_records'] and batch==len(raw.splitlines())
