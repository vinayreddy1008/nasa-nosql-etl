#!/usr/bin/env python3
"""Read-only Phase 1 correctness audit and reproducible Markdown evidence.

Usage (from repository root): python -m scripts.phase1_audit

This checks the two latest successful pipeline runs against the original raw NASA
files and their PostgreSQL results. It does not execute ETL, modify raw logs, or
update database records. Only on a complete PASS does it write
``docs/PHASE1_RESULTS.md``.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
FILES = ("NASA_access_log_Jul95", "NASA_access_log_Aug95")
BASELINE = {"total": 3_461_613, "malformed": 3_930, "q1": 288, "q2": 20, "q3": 1369}


class AuditError(RuntimeError):
    """A checked Phase 1 invariant did not hold."""


def require(ok: bool, message: str) -> None:
    if not ok:
        raise AuditError(message)


def raw_file_info(path: Path, block_size: int = 1024 * 1024) -> dict:
    """Count records correctly even if the final line has no trailing newline."""
    count = 0
    total_bytes = 0
    last = None
    digest = sha256()
    with path.open("rb") as stream:
        while block := stream.read(block_size):
            count += block.count(b"\n")
            total_bytes += len(block)
            last = block[-1]
            digest.update(block)
    if last is not None and last != 10:
        count += 1
    return {"name": path.name, "size_bytes": total_bytes, "records": count,
            "ends_with_newline": last == 10, "sha256": digest.hexdigest()}


def expected_batches(source_info: list[dict], batch_size: int) -> list[tuple[str, int]]:
    require(batch_size > 0, "Batch size must be positive")
    answer = []
    for item in source_info:
        full, remainder = divmod(item["records"], batch_size)
        answer.extend((item["name"], batch_size) for _ in range(full))
        if remainder:
            answer.append((item["name"], remainder))
    return answer


def sql_rows(conn, query: str, params=()):
    from psycopg.rows import dict_row
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        return list(cur.fetchall())


def ensure_result_metadata(rows: list[dict], run: dict, table: str) -> None:
    for r in rows:
        require(r["pipeline"] == run["pipeline"], f"{table}: pipeline metadata mismatch")
        require(r["batch_id"] == run["batch_count"], f"{table}: aggregate batch_id differs from final batch")
        require(r["executed_at"] is not None, f"{table}: missing execution timestamp")


def audit_database(conn, input_info: list[dict]) -> dict:
    """Raise on any mismatch; leave the SQL database completely unchanged."""
    expected_manifest = [{"name": x["name"], "size_bytes": x["size_bytes"]} for x in input_info]
    total_files = sum(x["records"] for x in input_info)
    require(total_files == BASELINE["total"],
            f"Input total changed: {total_files} vs recorded Phase 1 baseline {BASELINE['total']}")

    successful = sql_rows(conn, """SELECT run_id,pipeline,dataset_manifest,batch_size,batch_count,
       total_records,malformed_records,average_batch_size,runtime_seconds,
       started_at,finished_at,status FROM runs
       WHERE status='success' AND pipeline IN ('mongo','mapreduce')
       ORDER BY started_at DESC""")
    runs = {}
    for row in successful:
        runs.setdefault(row["pipeline"], row)
    require(set(runs) == {"mongo", "mapreduce"}, "Need successful MongoDB and MapReduce runs")
    require(runs["mongo"]["batch_size"] == runs["mapreduce"]["batch_size"],
            "Cannot compare different batch sizes")
    results = {}
    for pipeline in ("mongo", "mapreduce"):
        run = runs[pipeline]
        rid = run["run_id"]
        require(run["dataset_manifest"] == expected_manifest, f"{pipeline}: manifest differs from current official files")
        require(run["total_records"] == total_files, f"{pipeline}: record total mismatch")
        require(run["malformed_records"] == BASELINE["malformed"], f"{pipeline}: malformed baseline mismatch")
        require(run["runtime_seconds"] is not None and run["runtime_seconds"] > 0,
                f"{pipeline}: invalid runtime")
        require(run["finished_at"] is not None, f"{pipeline}: no end timestamp")
        require(run["started_at"] <= run["finished_at"], f"{pipeline}: inverted execution timestamps")
        n = run["total_records"]
        batch_size = int(run["batch_size"])
        expected = expected_batches(input_info, batch_size)
        batches = sql_rows(conn, """SELECT batch_id,source_file,input_count
                FROM batches WHERE run_id=%s ORDER BY batch_id""", (rid,))
        require(len(batches) == len(expected) == run["batch_count"],
                f"{pipeline}: number of batches incorrect")
        require(all(b["batch_id"] == i + 1 for i,b in enumerate(batches)),
                f"{pipeline}: batch IDs not sequential")
        require([(b["source_file"], b["input_count"]) for b in batches] == expected,
                f"{pipeline}: batch records or per-file boundaries incorrect")
        require(sum(b["input_count"] for b in batches) == n,
                f"{pipeline}: batch sum does not equal total input")
        avg = Decimal(n) / Decimal(len(batches))
        require(abs(Decimal(run["average_batch_size"]) - avg) <= Decimal("0.00005"),
                f"{pipeline}: average batch size is incorrect")

        q1 = sql_rows(conn, """SELECT pipeline,batch_id,executed_at,log_date,status_code,
                request_count,total_bytes FROM q1_daily_traffic
                WHERE run_id=%s ORDER BY log_date,status_code""", (rid,))
        q2 = sql_rows(conn, """SELECT pipeline,batch_id,executed_at,resource_path,
                request_count,total_bytes,distinct_host_count FROM q2_top_resources
                WHERE run_id=%s ORDER BY request_count DESC,resource_path ASC""", (rid,))
        q3 = sql_rows(conn, """SELECT pipeline,batch_id,executed_at,log_date,log_hour,
                error_request_count,total_request_count,error_rate,distinct_error_hosts
                FROM q3_hourly_errors WHERE run_id=%s ORDER BY log_date,log_hour""", (rid,))
        for tbl, rows in (("q1_daily_traffic", q1), ("q2_top_resources", q2), ("q3_hourly_errors", q3)):
            require(bool(rows), f"{pipeline}: empty required table {tbl}")
            ensure_result_metadata(rows, run, tbl)

        require(len(q1) == BASELINE["q1"] and len(q2) == BASELINE["q2"] and len(q3) == BASELINE["q3"],
                f"{pipeline}: output row-count baseline mismatch")
        good = n - run["malformed_records"]
        require(sum(r["request_count"] for r in q1) == good,
                f"{pipeline}: Q1 did not account for every valid request")
        require(sum(r["total_request_count"] for r in q3) == good,
                f"{pipeline}: Q3 did not account for every valid request")
        require(all(r["request_count"] > 0 and r["total_bytes"] >= 0 for r in q1),
                f"{pipeline}: Q1 contains negative or zero request counts")
        require(all(r["request_count"] > 0 and r["total_bytes"] >= 0 and
                    1 <= r["distinct_host_count"] <= r["request_count"] for r in q2),
                f"{pipeline}: Q2 contains invalid counts")
        require(q2 == sorted(q2, key=lambda r:(-r["request_count"],r["resource_path"])),
                f"{pipeline}: incorrect top-20 ordering")
        for row in q3:
            err, total = row["error_request_count"], row["total_request_count"]
            require(0 <= err <= total and total > 0, f"{pipeline}: invalid hourly error counts")
            require(0 <= row["distinct_error_hosts"] <= err, f"{pipeline}: distinct error hosts exceeds errors")
            require(0 <= row["log_hour"] <= 23, f"{pipeline}: invalid log hour")
            require(abs(Decimal(row["error_rate"]) - Decimal(err) / Decimal(total)) < Decimal("0.000000000001"),
                    f"{pipeline}: incorrect stored error rate")

        results[pipeline] = dict(run=run, batches=batches, q1=q1, q2=q2, q3=q3)

    for key, columns in (
        ("q1", ("log_date", "status_code", "request_count", "total_bytes")),
        ("q2", ("resource_path", "request_count", "total_bytes", "distinct_host_count")),
        ("q3", ("log_date", "log_hour", "error_request_count", "total_request_count", "error_rate", "distinct_error_hosts")),
    ):
        rows_a = [tuple(r[c] for c in columns) for r in results["mongo"][key]]
        rows_b = [tuple(r[c] for c in columns) for r in results["mapreduce"][key]]
        require(rows_a == rows_b, f"Cross-pipeline {key.upper()} values differ")

    a, b = results["mongo"]["batches"], results["mapreduce"]["batches"]
    require([(r["source_file"],r["input_count"]) for r in a] ==
            [(r["source_file"],r["input_count"]) for r in b],
            "Batch partitioning differs between backends")
    return results


def render_results(results: dict, info: list[dict]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Phase 1 — Verified execution evidence", "",
        f"Generated from a **read-only PostgreSQL + raw NASA log audit** at {now}.",
        "This document reports observed runs; it is not a fabricated benchmark or a multi-node Hadoop claim.", "",
        "## Official input", "",
        "| Raw file | Bytes | Records | SHA-256 |", "|---|---:|---:|---|",
    ]
    for x in info:
        lines.append(f"| `{x['name']}` | {x['size_bytes']:,} | {x['records']:,} | `{x['sha256']}` |")
    lines += ["", "Both runs used these two official raw files and the same batch size.", "",
              "**Line-count reconciliation:** The Internet Traffic Archive description quotes "
              "3,461,612 requests; the audited files contain 3,461,613 line-delimited "
              "input records when an unterminated final record is included. This "
              "one-record discrepancy must be explained, not silently discarded. "
              "Batching counts every physical input record, including malformed ones.", "",
              "## Successful runs", "",
              "| Backend | Run ID | Records | Malformed | Batches | Batch size | Avg. batch size | Runtime (s) |",
              "|---|---|---:|---:|---:|---:|---:|---:|"]
    for pipeline in ("mongo","mapreduce"):
        r = results[pipeline]["run"]
        lines.append(f"| {pipeline} | `{r['run_id']}` | {r['total_records']:,} | "
                     f"{r['malformed_records']:,} | {r['batch_count']} | {r['batch_size']:,} | "
                     f"{r['average_batch_size']} | {r['runtime_seconds']} |")
    lines += ["", "## Query correctness", "",
              "| Required query | MongoDB rows | MapReduce rows | Checked values |",
              "|---|---:|---:|---|"]
    for label,key in (("Q1: Daily traffic","q1"),("Q2: Top 20 resources","q2"),("Q3: Hourly errors","q3")):
        n = len(results["mongo"][key])
        lines.append(f"| {label} | {n} | {n} | Exact equality, including Q3 stored error rates |")
    lines += ["", "Verified: file manifests; 100% batch IDs and counts; per-file partial batches; "
                  "sum of valid requests; Q2 ranking; Q3 rates and error-host bounds; "
                  "all query values; pipeline/run/batch timestamps.", "",
              "## Interpretation and limitations", "",
              "- Hadoop used **local JobRunner / Hadoop Streaming**, not HDFS/YARN distributed-cluster mode.",
              "- Runtimes are individual observed executions. Multiple controlled repetitions "
                  "would be needed to make reliable performance claims.",
              "- The MongoDB and MapReduce pipelines share a common parsing contract; matching "
                  "results alone do not independently establish that every unusual HTTP request "
                  "is classified according to the professor's intended interpretation.",
              "- SHA-256 hashes are for the current local official files. Older run manifests "
                  "recorded file names and sizes, not hashes at execution time.",
              "- The report is generated by `python -m scripts.phase1_audit` and is not itself "
                  "an ETL execution artifact.", ""]
    return "\n".join(lines)


def main() -> int:
    try:
        info = []
        for name in FILES:
            path = ROOT / "data" / name
            require(path.is_file(), f"Missing official raw log: {path}")
            item = raw_file_info(path)
            info.append(item)
            print(f"INPUT {name}: {item['records']:,} records, {item['size_bytes']:,} bytes; "
                  f"final-newline={item['ends_with_newline']}; sha256={item['sha256'][:16]}...", flush=True)
        from app.store import db_connect
        with db_connect() as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            results = audit_database(conn, info)
        print("PASS: same official input, sequential and accurate batches, full Q1/Q2/Q3 equality", flush=True)
        for pipeline in ("mongo", "mapreduce"):
            r = results[pipeline]["run"]
            print(f"PASS {pipeline}: {r['run_id']} runtime={r['runtime_seconds']}s "
                  f"Q1={len(results[pipeline]['q1'])} Q2={len(results[pipeline]['q2'])} Q3={len(results[pipeline]['q3'])}", flush=True)
        destination = ROOT / "docs" / "PHASE1_RESULTS.md"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(render_results(results, info), encoding="utf-8")
        print(f"PASS: evidence saved to {destination.relative_to(ROOT)}", flush=True)
        return 0
    except (AuditError, OSError, ValueError, KeyError) as exc:
        print(f"FAIL Phase 1 audit: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
