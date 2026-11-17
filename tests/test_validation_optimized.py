"""No correctness guard may disappear under Python's optimization switch."""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_missing_q1_requests_rejected_with_optimized_python():
    program = '''
from app.validation import validate_result
validate_result({
  'total_records': 2, 'malformed_records': 0,
  'batches': [{'batch_id': 1, 'source_file': 'official', 'input_count': 2}],
  'q1': [{'log_date': '1995-07-01', 'status_code': 200, 'request_count': 1, 'total_bytes': 8}],
  'q2': [{'resource_path': '/a', 'request_count': 2, 'total_bytes': 8, 'distinct_host_count': 1}],
  'q3': [{'log_date': '1995-07-01', 'log_hour': 0, 'error_request_count': 1,
          'total_request_count': 2, 'error_rate': .5, 'distinct_error_hosts': 1}],
})
'''
    check = subprocess.run([sys.executable, '-O', '-c', program], cwd=ROOT,
                           capture_output=True, text=True)
    assert check.returncode != 0
    assert 'Q1 loss or duplication detected' in check.stderr
