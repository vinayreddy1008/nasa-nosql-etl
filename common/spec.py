"""Canonical, pipeline-independent contract. For assertions and test oracle only.

No production backend may call parse_line() to do its ETL; the MongoDB backend
uses MongoDB aggregation operators, and Hadoop MapReduce uses mapper/reducer
executables. The reference is also used in offline tests.
"""
from __future__ import annotations

import datetime as dt
import re
from collections import Counter, defaultdict

# Byte count is OPTIONAL (missing or '-' => 0). Only the official input is used
# for real experiments; synthetic lines are exclusively for developer tests.
LOG_PATTERN = (
    r'^(\S+) - - \[([0-9]{2})/([A-Za-z]{3})/([0-9]{4}):'
    r'([0-9]{2}):([0-9]{2}):([0-9]{2}) ([+-][0-9]{4})\] '
    r'"([^"]*)" ([0-9]{3})(?:\s+([0-9]+|-))?$'
)
REQUEST_PATTERN = r'^([A-Za-z]+) (\S+)(?: (HTTP/[0-9]+(?:\.[0-9]+)?))?$'
LOG_RE = re.compile(LOG_PATTERN)
REQUEST_RE = re.compile(REQUEST_PATTERN)
MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def parse_line(line: str) -> dict | None:
    """Oracle, never used for actual ETL execution.

    Log date/hour are the clock values recorded in the log, not UTC-converted.
    Resource path excludes the URL query component; fragments are preserved.
    """
    match = LOG_RE.fullmatch(line.rstrip('\n\r'))
    if not match:
        return None
    host, day, month, year, hour, minute, second, offset, request, status, nbytes = match.groups()
    req = REQUEST_RE.fullmatch(request)
    if not req or month not in MONTHS:
        return None
    method, target, protocol = req.groups()
    try:
        log_dt = dt.datetime(int(year), MONTHS.index(month) + 1, int(day),
                             int(hour), int(minute), int(second))
    except ValueError:
        return None
    return {
        'host': host,
        'timestamp': f'{day}/{month}/{year}:{hour}:{minute}:{second} {offset}',
        'log_date': log_dt.date().isoformat(),
        'log_hour': int(hour),
        'http_method': method,
        'resource_path': target.split('?', 1)[0],
        'protocol_version': protocol,
        'status_code': int(status),
        'bytes_transferred': 0 if nbytes in (None, '-') else int(nbytes),
    }


def reference_aggregate(lines: list[str]) -> dict:
    """Small-data test oracle; never used to produce a real pipeline's results."""
    q1, q2, q3 = defaultdict(lambda: [0, 0]), {}, {}
    malformed = 0
    for line in lines:
        d = parse_line(line)
        if d is None:
            malformed += 1
            continue
        k1 = (d['log_date'], d['status_code'])
        q1[k1][0] += 1
        q1[k1][1] += d['bytes_transferred']
        k2 = d['resource_path']
        if k2 not in q2:
            q2[k2] = [0, 0, set()]
        q2[k2][0] += 1
        q2[k2][1] += d['bytes_transferred']
        q2[k2][2].add(d['host'])
        k3 = (d['log_date'], d['log_hour'])
        if k3 not in q3:
            q3[k3] = [0, 0, set()]
        q3[k3][1] += 1
        if 400 <= d['status_code'] <= 599:
            q3[k3][0] += 1
            q3[k3][2].add(d['host'])
    return {
        'q1': [dict(log_date=a, status_code=b, request_count=v[0], total_bytes=v[1])
               for (a, b), v in sorted(q1.items())],
        'q2': [dict(resource_path=p, request_count=v[0], total_bytes=v[1],
                    distinct_host_count=len(v[2]))
               for p, v in sorted(q2.items(), key=lambda kv: (-kv[1][0], kv[0]))[:20]],
        'q3': [dict(log_date=a, log_hour=b, error_request_count=v[0],
                    total_request_count=v[1], error_rate=v[0] / v[1],
                    distinct_error_hosts=len(v[2]))
               for (a, b), v in sorted(q3.items())],
        'malformed_records': malformed,
        'total_records': len(lines),
    }
