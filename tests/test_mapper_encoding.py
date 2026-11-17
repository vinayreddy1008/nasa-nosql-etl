"""Verify that non-UTF-8 NASA-style records do not crash the mapper."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def test_mapper_preserves_non_utf8_records():
    raw = (
        b'site\x80.example - - [01/Jul/1995:00:00:01 -0400] "GET /valid HTTP/1.0" 200 5\n'
        b'site.example - - [01/Jul/1995:00:00:02 -0400] "BROKEN\xfe\x80" 400 -\n'
        b'bad\x81record\n'
    )

    result = subprocess.run(
        [sys.executable, str(ROOT / "pipelines/mapreduce/mapper.py")],
        input=raw,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=ROOT,
        check=True,
    )

    output = result.stdout.decode("utf-8")

    assert "Q1\x1f1995-07-01\x1f200\t5" in output
    assert output.count("M\x1fBAD\t1") == 2
    assert any(
        line.startswith("B\x1f") and line.endswith("\t3")
        for line in output.splitlines()
    )
