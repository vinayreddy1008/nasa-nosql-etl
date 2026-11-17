"""PostgreSQL reporting schema + atomic aggregate loading."""
from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


def db_connect():
    import psycopg
    return psycopg.connect(os.environ.get('PG_DSN', 'postgresql://nosql:nosql@localhost:5437/nosql_analytics'))


def setup_db():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute((Path(__file__).resolve().parents[1] / 'sql/schema.sql').read_text())
    print('PostgreSQL schema installed (non-destructive)')


def save_aggregates(run_id: str, pipeline: str, manifest: list, batch_size: int,
                    started_at: datetime, result: dict):
    """First commit writes all aggregates atomically. Runtime is finalized afterward."""
    now = datetime.now(timezone.utc)
    batch_id = len(result['batches'])  # complete-dataset aggregate, final batch identifier
    n = result['total_records']
    with db_connect() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute('''INSERT INTO runs
                  (run_id,pipeline,dataset_manifest,batch_size,batch_count,total_records,
                   malformed_records,average_batch_size,started_at,status)
                   VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,'loading')''',
                    (run_id,pipeline,json.dumps(manifest),batch_size,batch_id,n,
                     result['malformed_records'], Decimal(n)/Decimal(batch_id),started_at))
                cur.executemany('''INSERT INTO batches(run_id,batch_id,source_file,input_count)
                                   VALUES (%s,%s,%s,%s)''',
                    [(run_id,b['batch_id'],b['source_file'],b['input_count']) for b in result['batches']])
                cur.executemany('''INSERT INTO q1_daily_traffic
                 (run_id,pipeline,batch_id,executed_at,log_date,status_code,request_count,total_bytes)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                    [(run_id,pipeline,batch_id,now,r['log_date'],r['status_code'],r['request_count'],r['total_bytes'])
                     for r in result['q1']])
                cur.executemany('''INSERT INTO q2_top_resources
                 (run_id,pipeline,batch_id,executed_at,resource_path,request_count,total_bytes,distinct_host_count)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                    [(run_id,pipeline,batch_id,now,r['resource_path'],r['request_count'],
                      r['total_bytes'],r['distinct_host_count']) for r in result['q2']])
                cur.executemany('''INSERT INTO q3_hourly_errors
                 (run_id,pipeline,batch_id,executed_at,log_date,log_hour,error_request_count,
                  total_request_count,error_rate,distinct_error_hosts)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    [(run_id,pipeline,batch_id,now,r['log_date'],r['log_hour'],r['error_request_count'],
                      r['total_request_count'],Decimal(r['error_request_count'])/Decimal(r['total_request_count']),
                      r['distinct_error_hosts']) for r in result['q3']])


def finish_run(run_id: str, runtime: float):
    with db_connect() as conn:
        conn.execute('''UPDATE runs SET status='success',finished_at=NOW(),runtime_seconds=%s
                        WHERE run_id=%s''', (runtime,run_id))


def show_report(limit: int = 8):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT run_id,pipeline,status,total_records,malformed_records,
                          batch_count,batch_size,average_batch_size,runtime_seconds
                          FROM runs ORDER BY started_at DESC LIMIT %s''',(limit,))
            rows = cur.fetchall()
            if not rows:
                print('No runs yet'); return
            for run_id,pipeline,status,n,bad,bc,bs,avg,rt in rows:
                print(f'\nRUN {run_id} | pipeline={pipeline} | status={status}')
                print(f'  records={n} malformed={bad} last_batch_id={bc} nonempty_batches={bc} batch_size={bs} '
                      f'average_batch_size={avg} runtime_seconds={rt}')
                for table,cols in [
                    ('q1_daily_traffic','log_date,status_code,request_count,total_bytes'),
                    ('q2_top_resources','resource_path,request_count,total_bytes,distinct_host_count'),
                    ('q3_hourly_errors','log_date,log_hour,error_request_count,total_request_count,error_rate,distinct_error_hosts')]:
                    sort=('request_count DESC, resource_path ASC' if table=='q2_top_resources'
                          else '1,2')
                    cur.execute(f'SELECT {cols} FROM {table} WHERE run_id=%s ORDER BY {sort} LIMIT 5', (run_id,))
                    print(f'  {table} preview:')
                    for r in cur.fetchall(): print('   ',r)
                cur.execute('SELECT batch_id,source_file,input_count FROM batches WHERE run_id=%s ORDER BY batch_id DESC LIMIT 3',(run_id,))
                print('  Last batch details:',cur.fetchall())


def compare_latest():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT run_id,pipeline,dataset_manifest,batch_size,runtime_seconds
              FROM runs WHERE status='success' AND pipeline IN ('mongo','mapreduce')
              ORDER BY started_at DESC''')
            latest={}
            for row in cur.fetchall(): latest.setdefault(row[1], row)
            if len(latest) != 2:
                print('Need successful runs from both mongo and mapreduce'); return False
            mongo,mr=latest['mongo'],latest['mapreduce']
            if mongo[2] != mr[2]:
                print('FAIL: Input manifest differs; comparison would be unfair')
                return False
            if mongo[3] != mr[3]:
                print('FAIL: Batch sizes differ; comparison would be unfair')
                return False
            for table, cols in [
                ('q1_daily_traffic','log_date,status_code,request_count,total_bytes'),
                ('q2_top_resources','resource_path,request_count,total_bytes,distinct_host_count'),
                ('q3_hourly_errors','log_date,log_hour,error_request_count,total_request_count,error_rate,distinct_error_hosts')]:
                datasets=[]
                for run in (mongo,mr):
                    cur.execute(f'SELECT {cols} FROM {table} WHERE run_id=%s ORDER BY 1,2',(run[0],))
                    datasets.append(cur.fetchall())
                if datasets[0] != datasets[1]:
                    print(f'FAIL: Wrong output / schema mismatch in {table}')
                    return False
                print(f'PASS {table}: {len(datasets[0])} identical rows')
            print('PASS: identical datasets, batch sizes and all Q1/Q2/Q3 integer results')
            print(f'Runtimes: MongoDB {mongo[4]}s; MapReduce {mr[4]}s')
            return True
