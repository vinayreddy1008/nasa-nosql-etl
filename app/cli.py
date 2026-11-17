"""Phase 1 single-entrypoint CLI. One backend per run, no shared ETL transform."""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
EXPECTED=('NASA_access_log_Jul95','NASA_access_log_Aug95')


def main():
    cli=argparse.ArgumentParser(description='NASA HTTP Log Multi-Pipeline ETL — Phase 1')
    cmd=cli.add_subparsers(dest='command',required=True)
    cmd.add_parser('download',help='Download and decompress official NASA archives only')
    cmd.add_parser('setup-db',help='Create PostgreSQL tables without dropping existing data')
    run=cmd.add_parser('run',help='Execute one selected backend end to end')
    run.add_argument('--pipeline',choices=['mongo','mapreduce','pig','hive'],required=True,
                     help='MongoDB or MapReduce implemented in Phase 1; Pig/Hive reserved for Phase 2')
    run.add_argument('--batch-size',type=int,default=100000)
    run.add_argument('--data-dir',type=Path,default=ROOT/'data')
    cmd.add_parser('report',help='Read stored results from PostgreSQL')
    cmd.add_parser('compare',help='Require identical data/batch size/results across pipelines')
    args=cli.parse_args()
    if args.command=='download':
        from app.data import download
        download(ROOT/'data');return
    from app.store import setup_db,show_report,compare_latest
    if args.command=='setup-db':setup_db();return
    if args.command=='report':show_report();return
    if args.command=='compare':
        if not compare_latest():sys.exit(2)
        return
    if args.pipeline in ('pig','hive'):
        cli.error(f'{args.pipeline} is reserved for Phase 2; Phase 1 implements mongo and mapreduce')
    if args.batch_size<1:cli.error('batch-size must be positive')
    data_dir=args.data_dir.resolve()
    paths=[data_dir/name for name in EXPECTED]
    for path in paths:
        if not path.is_file():cli.error(f'Official raw NASA file missing: {path}. Run download first.')
        if path.stat().st_size<=0:cli.error(f'Empty NASA input file: {path}')
    manifest=[{'name':p.name,'size_bytes':p.stat().st_size} for p in paths]
    # One controlled path for the same two official original files across all runs.
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:8]
    started_at=datetime.now(timezone.utc)
    print(f'START run_id={run_id} pipeline={args.pipeline} batch_size={args.batch_size}',flush=True)
    print('Dataset manifest:',json.dumps(manifest),flush=True)
    start=time.perf_counter()  # before selected pipeline begins reading any raw input
    if args.pipeline=='mongo':
        from pipelines.mongo.backend import execute
    else:
        from pipelines.mapreduce.backend import execute
    result=execute(paths,args.batch_size,run_id)
    from app.validation import validate_result
    validate_result(result)
    print(f'Backend validated: total={result["total_records"]} '
          f'malformed={result["malformed_records"]} batches={len(result["batches"])} '
          f'Q1={len(result["q1"])} Q2={len(result["q2"])} Q3={len(result["q3"])}',flush=True)
    from app.store import save_aggregates,finish_run
    save_aggregates(run_id,args.pipeline,manifest,args.batch_size,started_at,result)
    # Final aggregates have committed to PostgreSQL; stop timer NOW.
    duration=time.perf_counter()-start
    finish_run(run_id,duration)
    print(f'SUCCESS run_id={run_id}, runtime_seconds={duration:.6f}',flush=True)
    print('Inspect results with: python -m app.cli report',flush=True)

if __name__=='__main__':main()
