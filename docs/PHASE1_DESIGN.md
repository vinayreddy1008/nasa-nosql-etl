# Phase 1 Architecture and Design — NASA HTTP Logs

## Specification checklist

- [x] One user selects one pipeline per run (CLI)
- [x] Proposed 4-backend architecture with MongoDB and MapReduce Phase 1 code
- [x] Raw NASA Jul95/Aug95 official logs only, unchanged after gzip decompression
- [x] Same logical parsing specification, all mandatory fields
- [x] Count malformed lines, treat missing byte values or `-` as zero
- [x] Q1 daily traffic summary by date/status
- [x] Q2 globally top-20 resources with distinct hosts
- [x] Q3 hourly error rate, total requests and distinct error hosts
- [x] Record-count batching and last-partial batch handling
- [x] PostgreSQL result schema with run, pipeline, batch and execution times
- [x] Runtime measured through first committed relational results
- [x] Clear reporting and fairness validation
- [x] **Actual live end-to-end successful execution on the group Mac**
- [x] Verify both pipelines produce exactly equal results on official full data
- [ ] Document replicated runtime experiments, full machine specs and screenshots (initial measured runtimes in `PHASE1_RESULTS.md`)
- [ ] Resolve architectural review questions with instructor

## Data flow

Input NASAs raw text -> CLI -> selected backend only. MongoDB loads raw text into Mongo staging collection, uses aggregation expressions to parse into typed structured collection, then runs Q1/Q2/Q3 through MongoDB grouping. Hadoop uses NLineInputFormat records/batch with Hadoop Streaming mapper/reducer, followed by second reducer for global top 20. Both create the same output fields and return them to a common PostgreSQL loader. Report reads SQL, not staging collections.

## Relation schema and semantics

`runs`: run_id, chosen backend, dataset manifest, total raw records, malformed records, number of nonempty batches, chosen batch size, average batch size, start/end timestamps, runtime.

`batches`: per-run sequential batch_id (from 1), source file and number of raw input records.

`q1_daily_traffic`, `q2_top_resources`, `q3_hourly_errors`: exactly required logical query outputs, plus pipeline/run_id/batch_id/executed_at columns. The batch_id on full-run aggregates identifies the **last completed batch**, not an aggregate of that batch alone. The per-batch counts are in `batches`.

## Reconciliation rules

- `sum(Q1.request_count)` = raw input count - malformed count.
- `sum(Q3.total_request_count)` = raw input count - malformed count.
- Q3 error requests <= all requests, error rate = errors / total.
- `sum(batch.input_count)` = raw input count; all batch IDs sequential, no zero-size batch.
- Both runs must have identical input manifests and batch sizes before comparison.
- Compare exact integer results Q1/Q2/Q3; rate can be recomputed from exact count integers if required.

## Instructor questions / documented interpretations

1. Batch boundaries reset on changing files, although batch IDs continue globally. Ask whether the instructor instead expects batches to span concatenated files.
2. Hadoop local JobRunner executes real MapReduce shuffle/reduce tasks. Ask whether Phase 1 requires HDFS/YARN or accepts local mode as a prototype.
3. The assignment says `resource path`; we interpret that as the target before the URL query `?` and preserve case.
4. Queries are computed across the entire combined dataset; tables carry the final batch ID to satisfy the metadata requirement. Confirm this interpretation if required.
5. Only official NASA logs are used for experiment runs. Synthetic strings appear *solely in unit tests*, not ETL experiments.

## Phase 2 extension plan

Add `pipelines/pig` and `pipelines/hive` adapters that accept the exact same official raw file paths, batch size and run ID. Each adapter implements the same parser, Q1–Q3 and malformed/batch counts in Pig Latin or HiveQL. PostgreSQL schema and reporting stay unchanged. The original controller enforces consistent semantics and comparisons without implementing the backend ETL itself.

## Evidence log (fill only after genuinely running)

| Run | Actual result | Evidence filename |
|---|---|---|
| `pytest -q` | passed on group Mac (13 tests before final audit additions) | Terminal output |
| `docker compose ps` | MongoDB and PostgreSQL healthy; Hadoop started | Terminal output |
| MongoDB full dataset | success | `PHASE1_RESULTS.md` |
| MapReduce full dataset | success | `PHASE1_RESULTS.md` |
| SQL reporting | success | `PHASE1_RESULTS.md` |
| Comparison all 3 queries | full equality | `PHASE1_RESULTS.md` |

Do not fabricate screenshots or contribution histories. Record the actual executed commands, result IDs and team contributions.
