#!/usr/bin/env python3
"""Hadoop Streaming mapper: one NLineInputFormat input split is one batch.

Importing the common parsing contract here is legitimate: this code runs inside
Hadoop mapper tasks. The controller does not call it.
"""
import os
import sys
# Local tests run mapper.py by path; Hadoop distributes spec.py alongside it.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from common.spec import parse_line
except ModuleNotFoundError:  # Hadoop task localization flattens the -files payload
    from spec import parse_line

SEP='\x1f'


def emit(key, value):
    sys.stdout.write(key+'\t'+value+'\n')


def main():
    count=0
    for raw_line in sys.stdin.buffer:
        prefix, tab, value = raw_line.partition(b"\t")
        payload = value if tab and prefix.isdigit() else raw_line
        line = payload.decode("latin-1")
        count+=1
        record=parse_line(line)
        if record is None:
            emit('M'+SEP+'BAD','1')
            continue
        d=record
        emit('Q1'+SEP+d['log_date']+SEP+str(d['status_code']),str(d['bytes_transferred']))
        emit('Q2'+SEP+d['resource_path'],str(d['bytes_transferred'])+SEP+d['host'])
        error='1' if 400 <= d['status_code'] <= 599 else '0'
        emit('Q3'+SEP+d['log_date']+SEP+str(d['log_hour']),error+SEP+d['host'])
    if count:
        source=(os.getenv('mapreduce_map_input_file') or os.getenv('map_input_file')
                or os.getenv('mapreduce_input_file') or 'unknown')
        split=(os.getenv('mapreduce_map_input_start') or os.getenv('map_input_start')
               or os.getenv('mapreduce_task_attempt_id') or os.getenv('mapred_task_id') or 'unknown')
        emit('B'+SEP+source+SEP+str(split),str(count))

if __name__=='__main__':main()
