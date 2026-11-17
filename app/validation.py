"""Run-level safety gates; reject incomplete ETL before database persistence."""
from math import isclose


def validate_result(result: dict) -> None:
    batches = result['batches']
    n = result['total_records']
    bad = result['malformed_records']
    assert n > 0, 'No input records were processed'
    assert 0 <= bad <= n, 'Malformed count outside allowed range'
    if bad == n or not result['q1'] or not result['q2'] or not result['q3']:
        raise ValueError(
            "No valid records or missing required query output; refusing successful run"
        )
    assert sum(b['input_count'] for b in batches) == n, 'Input batch counts do not sum to total records'
    assert all(b['batch_id'] == i + 1 and b['input_count'] > 0
               for i, b in enumerate(batches)), 'Batch IDs must be consecutive, starting at 1'
    assert sum(q['request_count'] for q in result['q1']) == n - bad, 'Q1 loss or duplication detected'
    assert sum(q['total_request_count'] for q in result['q3']) == n - bad, 'Q3 loss or duplication detected'
    assert len(result['q2']) <= 20, 'Q2 must contain at most 20 rows'
    assert result['q2'] == sorted(result['q2'], key=lambda r: (-r['request_count'], r['resource_path'])), 'Q2 order violation'
    for r in result['q3']:
        assert 0 <= r['error_request_count'] <= r['total_request_count'], 'Invalid error count'
        assert isclose(r['error_rate'], r['error_request_count']/r['total_request_count'], abs_tol=1e-9), 'Invalid error rate'
