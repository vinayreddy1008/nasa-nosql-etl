"""Explicit result invariants, active even under Python -O optimization."""
from math import isclose, isfinite


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_result(result: dict) -> None:
    batches = result['batches']
    n = result['total_records']
    bad = result['malformed_records']
    _require(n > 0, 'No input records were processed')
    _require(0 <= bad <= n, 'Malformed count outside allowed range')
    _require(bad != n and bool(result['q1']) and bool(result['q2']) and bool(result['q3']),
             'No valid records / missing required query output; refusing successful run')
    _require(sum(b['input_count'] for b in batches) == n,
             'Input batch counts do not sum to total records')
    _require(all(b['batch_id'] == i + 1 and 0 < b['input_count']
                 for i, b in enumerate(batches)),
             'Batch IDs must be consecutive, starting at 1, with nonempty batches')
    _require(sum(q['request_count'] for q in result['q1']) == n - bad,
             'Q1 loss or duplication detected')
    _require(sum(q['total_request_count'] for q in result['q3']) == n - bad,
             'Q3 loss or duplication detected')
    _require(len(result['q2']) <= 20,
             'Q2 must contain at most 20 rows')
    _require(result['q2'] == sorted(result['q2'],
             key=lambda r: (-r['request_count'], r['resource_path'])),
             'Q2 order violation')
    for r in result['q3']:
        error_count = r['error_request_count']
        total = r['total_request_count']
        _require(0 <= error_count <= total and total > 0, 'Invalid error count')
        rate = float(r['error_rate'])
        _require(isfinite(rate) and isclose(rate, error_count / total, abs_tol=1e-9),
                 'Invalid error rate')
