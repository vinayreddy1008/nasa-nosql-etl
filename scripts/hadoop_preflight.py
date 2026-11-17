"""Verify Hadoop Streaming with synthetic test data."""
from pathlib import Path
from uuid import uuid4
import shutil

from pipelines.mapreduce.backend import _job

ROOT = Path(__file__).resolve().parents[1]
relative = f"work/probes/input_protocol_{uuid4().hex[:8]}"
folder = ROOT / relative
folder.mkdir(parents=True, exist_ok=False)

raw = (
    b'a.example - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" 200 8\n'
    b'b.example - - [01/Jul/1995:00:00:02 -0400] "GET /a HTTP/1.0" 404 -\n'
    b'broken request\n'
    b'c.example - - [01/Jul/1995:00:00:04 -0400] "GET /b HTTP/1.0" 503 2\n'
    b'a.example - - [01/Jul/1995:00:00:05 -0400] "POST /b HTTP/1.1" 201 12\n'
)

(folder / "input.log").write_bytes(raw)
stage = relative + "/stage1"

try:
    _job(
        [relative + "/input.log"],
        stage,
        "mapper.py",
        "reducer.py",
        [
            "pipelines/mapreduce/mapper.py",
            "pipelines/mapreduce/reducer.py",
            "common/spec.py",
        ],
        2,
    )

    counts = {"valid": 0, "bad": 0, "batch_total": 0}

    for file in (ROOT / stage).glob("part-*"):
        for line in file.read_text().splitlines():
            parts = line.split("\t")

            if parts[0] == "Q1":
                counts["valid"] += int(parts[3])
            elif parts[0] == "M":
                counts["bad"] += int(parts[2])
            elif parts[0] == "B":
                counts["batch_total"] += int(parts[3])

    print("Hadoop integration preflight:", counts)

    expected = {"valid": 4, "bad": 1, "batch_total": 5}

    if counts != expected:
        raise RuntimeError(
            f"Input protocol mismatch; inspect {folder}"
        )

except Exception:
    print("Diagnostic files preserved:", folder)
    raise
else:
    shutil.rmtree(folder)
    print("PASS: Hadoop input, parsing and batching")
