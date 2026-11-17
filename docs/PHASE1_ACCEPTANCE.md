# Phase 1 technical acceptance review

## Evidence-backed status

The two Phase 1 execution choices—MongoDB and Apache Hadoop Streaming
MapReduce—have been run against both official NASA HTTP trace files. Results
are stored in PostgreSQL and can be read using the reporting CLI. The full-data
audit confirmed matching results for Q1 (288 rows), Q2 (20 rows), and Q3
(1,369 rows), and matching malformed-record and batch counts. The latest
measured run IDs and input checksums are recorded in `PHASE1_RESULTS.md`.

The six-run performance comparison is in `PHASE1_BENCHMARK.md`. Those times are
**local single-machine measurements**, not evidence of production throughput or
multi-node Hadoop performance.

## Technical acceptance matrix

| Requirement | Implemented/evidence | Limitation or interpretation |
|---|---|---|
| Official raw NASA files | July and August files, decompressed only; input hashes in audit | No manual preprocessing outside selected backends |
| Backend selection | Controller dispatches exactly one backend per run | Pig/Hive remain Phase 2 work |
| Two working pipelines | MongoDB aggregation and Hadoop Streaming mapper/reducer | Hadoop uses local JobRunner, not YARN/HDFS |
| Structured parsing | host, timestamp, local date/hour, HTTP method, path, protocol, status, bytes | Unusual request/protocol strings may be classified malformed; verify intended rules |
| Malformed accounting | 3,930 across 3,461,613 physical input lines | Includes eight-byte unterminated July tail fragment |
| Q1/Q2/Q3 | SQL results match row-by-row | Top-20 ties resolved by resource-path ascending |
| Batching | 35 record-count batches, IDs 1–35, average 98,903.2286 | Batches do not span July/August file boundaries |
| Reporting | PostgreSQL result tables plus run/batch metadata; `report` CLI | Aggregate row `batch_id` means the final batch ID, not a per-batch Q1/Q2/Q3 value |
| Runtime | Measured from selected backend start until aggregate SQL transaction commits | Follow-up status metadata update is outside timing |
| Correctness | Automated tests, audit of every batch, query comparison | No independent instructor-defined gold result set |
| Documentation | README, detailed architecture, results, benchmark, data quality, Git workflow | Presentation and live review are separate activities |

## Review risks to acknowledge (not conceal)

1. Confirm with the instructor that local Hadoop Streaming execution is acceptable for the Phase 1 prototype.
2. Confirm whether a configured batch may restart at each file boundary. To change this, both backends must change together.
3. Confirm intended parsing of nonstandard HTTP protocol values and query-string removal from resource paths.
4. Explain the convention used for the `batch_id` stored on full-run aggregate rows.
5. Phase 2 requires genuine Pig and Hive ETL implementations. Their presence in the controller's choices is a plan, not evidence they have been built.

## Verification commands

Run from the repository root with the project virtual environment active and
MongoDB/PostgreSQL/Hadoop containers already running:

```bash
python -m pytest -q
python -m app.cli compare
python -m scripts.phase1_audit
python -m app.cli report
```

The GitHub Actions workflow runs **offline regression tests only** after pushes
and on pull requests. It does not download the NASA dataset, start Docker, or
independently validate PostgreSQL; those live checks are performed locally.

Once these checks pass and the instructor-specific interpretations are
confirmed or explicitly disclosed, the Phase 1 *engineering baseline* can be
frozen. The formal review/presentation remains a distinct deliverable.
