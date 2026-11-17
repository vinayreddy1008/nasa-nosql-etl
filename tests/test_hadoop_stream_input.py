from pathlib import Path
import subprocess
import sys

import pytest
from app.validation import validate_result

ROOT = Path(__file__).resolve().parents[1]
MAPPER = ROOT / "pipelines/mapreduce/mapper.py"

VALID = (
    b'a.example - - [01/Jul/1995:00:00:01 -0400] '
    b'"GET /test HTTP/1.0" 200 7\n'
)


def test_plain_and_hadoop_offset_input():
    raw = VALID + b"0\t" + VALID + b"12345\tbad record\n"

    result = subprocess.run(
        [sys.executable, str(MAPPER)],
        input=raw,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=ROOT,
        check=True,
    )

    output = result.stdout.decode("utf-8")

    assert output.count("Q1\x1f1995-07-01\x1f200\t7") == 2
    assert output.count("M\x1fBAD\t1") == 1


def test_all_malformed_cannot_be_success():
    result = {
        "total_records": 3,
        "malformed_records": 3,
        "batches": [
            {"batch_id": 1, "source_file": "test", "input_count": 3}
        ],
        "q1": [],
        "q2": [],
        "q3": [],
    }

    with pytest.raises(ValueError, match="No valid records"):
        validate_result(result)
