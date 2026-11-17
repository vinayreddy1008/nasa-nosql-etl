"""Submit actual Hadoop Streaming MapReduce jobs via Docker; parse their output.

Hadoop's NLineInputFormat performs record-count batching *inside the engine*.
Using local JobRunner means Hadoop runs genuine map and reduce tasks on one
machine, a suitable Phase 1 prototype; replace with YARN for distributed runs.
"""
from __future__ import annotations
import os
import re
import subprocess
import uuid
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
SEP='\x1f'


def _run(script: str, *args):
    cmd=['docker','compose','--profile','mapreduce','exec','-T','hadoop','bash','-lc',script]
    p=subprocess.run(cmd,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    if p.returncode:
        print(p.stdout[-4000:],flush=True)
        print(p.stderr[-7000:],flush=True)
        raise RuntimeError('Hadoop job failed; inspect output above')
    print(p.stderr[-1200:],flush=True)


def _job(inputs: list[str], output: str, mapper: str, reducer: str, files: list[str],
         batch_size: int|None=None):
    # Exact paths are generated internally, not taken from shell-injection-prone user input.
    jar="$(find /opt/hadoop/share/hadoop/tools/lib /usr/local/hadoop/share/hadoop/tools/lib -name 'hadoop-streaming*.jar' 2>/dev/null | head -n 1)"
    if not files:raise ValueError('Missing mapper/reducer sources')
    from shlex import quote
    infile=' '.join('-input '+quote('file:///workspace/'+x) for x in inputs)
    attached=','.join('file:///workspace/'+f for f in files)
    cmd=(f'hadoop jar {jar} -D fs.defaultFS=file:/// '
         '-D mapreduce.framework.name=local -D mapreduce.job.reduces=1 '
         '-D mapreduce.job.user.classpath.first=true '
         + (f'-D mapred.line.input.format.linespermap={batch_size} '
            f'-D mapreduce.input.lineinputformat.linespermap={batch_size} '
            '' if batch_size else '')
         + '-files '+quote(attached)+' '
         + ('-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat ' if batch_size else '')
         + infile+' -output '+quote('file:///workspace/'+output)
         +' -mapper '+quote('python3 /workspace/pipelines/mapreduce/'+mapper)
         +' -reducer '+quote('python3 /workspace/pipelines/mapreduce/'+reducer))
    bash='set -e; export HADOOP_OPTS="${HADOOP_OPTS:-} -Djava.net.preferIPv4Stack=true"; jar='+jar+'; test -n "$jar"; '+cmd.replace('hadoop jar '+jar,'hadoop jar "$jar"')
    _run(bash)


def execute(files: list[Path], batch_size: int, run_id: str) -> dict:
    # The original unmodified log files are read directly by Hadoop.
    for f in files:
        if f.parent.resolve()!= (ROOT/'data').resolve():
            raise ValueError('MapReduce requires official input files in repo data/ directory')
    prefix='work/mapreduce_'+re.sub('[^A-Za-z0-9_]', '_',run_id)
    stage1=prefix+'/stage1'; stage2=prefix+'/top20'
    localdir=ROOT/prefix
    localdir.mkdir(parents=True,exist_ok=True)
    common='common/spec.py'
    # Hadoop distributes the executable mapper and parser module to each task.
    source_files=['pipelines/mapreduce/mapper.py','pipelines/mapreduce/reducer.py',common]
    # -files source paths are flattened to filenames in task workdirs.
    _job(['data/'+f.name for f in files],stage1,'mapper.py','reducer.py',source_files,batch_size)
    _job([stage1],stage2,'top_mapper.py','top_reducer.py',
         ['pipelines/mapreduce/top_mapper.py','pipelines/mapreduce/top_reducer.py'])
    output=dict(q1=[],q2=[],q3=[],malformed_records=0,batches=[],total_records=0)
    raw_batches=[]
    for file in sorted((ROOT/stage1).glob('part-*')):
        with open(file,encoding='utf-8') as f:
            for line in f:
                parts=line.rstrip('\n').split('\t')
                if parts[0]=='Q1':
                    _,date,status,n,amount=parts
                    output['q1'].append(dict(log_date=date,status_code=int(status),
                                             request_count=int(n),total_bytes=int(amount)))
                elif parts[0]=='Q3':
                    _,date,hour,err,n,rate,hosts=parts
                    output['q3'].append(dict(log_date=date,log_hour=int(hour),
                         error_request_count=int(err),total_request_count=int(n),
                         error_rate=float(rate),distinct_error_hosts=int(hosts)))
                elif parts[0]=='M':output['malformed_records']+=int(parts[2])
                elif parts[0]=='B':
                    _,source,split,n=parts
                    raw_batches.append((source,split,int(n)))
                elif parts[0]=='Q2':pass
                else:raise ValueError('Unrecognized Hadoop output: '+line)
    for file in sorted((ROOT/stage2).glob('part-*')):
        with open(file,encoding='utf-8') as f:
            for line in f:
                tag,path,n,total,hosts=line.rstrip('\n').split('\t')
                if tag!='Q2':raise ValueError('Invalid top20 output')
                output['q2'].append(dict(resource_path=path,request_count=int(n),
                              total_bytes=int(total),distinct_host_count=int(hosts)))
    # The split order is file path + byte offset. If Hadoop did not expose split
    # start, all IDs are still sequential, with a deterministic fallback sort.
    order={f.name:i for i,f in enumerate(files)}
    def batch_key(item):
        source,start,_=item
        name=Path(source).name
        return order.get(name,999), (0,int(start)) if start.isdigit() else (1,start)
    for i,(source,start,n) in enumerate(sorted(raw_batches,key=batch_key),1):
        output['batches'].append(dict(batch_id=i,source_file=Path(source).name,input_count=n))
        output['total_records']+=n
    output['q1'].sort(key=lambda r:(r['log_date'],r['status_code']))
    output['q2'].sort(key=lambda r:(-r['request_count'],r['resource_path']))
    output['q3'].sort(key=lambda r:(r['log_date'],r['log_hour']))
    return output
