#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p work/logs

echo "=== ENVIRONMENT ==="
docker compose --profile mapreduce ps
df -h .

echo "=== REGRESSION TESTS ==="
python -m pytest -q

echo "=== REPEATED ETL EXPERIMENTS ==="
for pipeline in mapreduce mongo mongo mapreduce; do
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    log="work/logs/trial_${pipeline}_${stamp}.log"

    echo "RUNNING $pipeline"

    python -m app.cli run \
        --pipeline "$pipeline" \
        --batch-size 100000 2>&1 | tee "$log"
done

echo "=== CROSS-PIPELINE COMPARISON ==="
python -m app.cli compare

echo "=== DEEP QUALITY AUDIT ==="
python -m scripts.phase1_audit

echo "=== RUNTIME STATISTICS ==="
python - <<'PY'
from collections import defaultdict
from statistics import mean, median, stdev
from pathlib import Path
from app.store import db_connect

with db_connect() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pipeline, run_id, runtime_seconds,
                   dataset_manifest, batch_size
            FROM runs
            WHERE status = 'success'
              AND pipeline IN ('mongo', 'mapreduce')
            ORDER BY started_at
        """)
        rows = cur.fetchall()

manifest = rows[-1][3]
batch_size = rows[-1][4]
groups = defaultdict(list)

for pipeline, run_id, runtime, dataset, size in rows:
    if dataset == manifest and size == batch_size:
        groups[pipeline].append((run_id, float(runtime)))

lines = [
    "# Phase 1 Runtime Benchmark",
    "",
    f"Batch size: {batch_size:,} records",
    "",
    "Execution environment: single-machine Docker; "
    "Hadoop local JobRunner.",
    "",
    "| Pipeline | Run ID | Runtime (s) |",
    "|---|---|---:|",
]

for pipeline in ("mongo", "mapreduce"):
    samples = groups[pipeline][-3:]

    if len(samples) < 3:
        raise SystemExit(f"Insufficient completed runs: {pipeline}")

    times = [runtime for _, runtime in samples]

    for run_id, runtime in samples:
        lines.append(f"| {pipeline} | `{run_id}` | {runtime:.3f} |")

    print(f"\n{pipeline.upper()}")
    print(f"Times: {[round(v, 3) for v in times]}")
    print(f"Mean: {mean(times):.3f} seconds")
    print(f"Median: {median(times):.3f} seconds")
    print(f"Std deviation: {stdev(times):.3f} seconds")

    lines += [
        "",
        f"**{pipeline}**",
        f"- Mean: {mean(times):.3f} s",
        f"- Median: {median(times):.3f} s",
        f"- Sample standard deviation: {stdev(times):.3f} s",
        "",
    ]

lines += [
    "## Limitations",
    "",
    "These are observed local-system runtimes. "
    "Cache effects, background processes, and container "
    "resources may affect the comparison. "
    "Results are not representative of a multi-node cluster.",
    "",
]

Path("docs/PHASE1_BENCHMARK.md").write_text(
    "\n".join(lines), encoding="utf-8"
)

print("\nPASS: benchmark report generated")
PY

echo "=== BENCHMARK COMPLETE ==="
