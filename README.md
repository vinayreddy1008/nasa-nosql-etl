# NASA HTTP Log Analytics — Phase 1 (clean-room implementation)

**Status:** Phase 1 MongoDB and Hadoop MapReduce successfully executed on the official full NASA logs, with PostgreSQL reporting and all three query results matching. See `docs/PHASE1_RESULTS.md` for independently audited run IDs and measured runtimes. Hadoop uses local JobRunner (not a distributed multi-node cluster).

## Mandatory Phase 1 deliverable

Two planned **real** execution backends: MongoDB aggregation and Apache Hadoop Streaming MapReduce; unified CLI; PostgreSQL result tables; deterministic three-query output schema; raw-record batching; malformed counts; runtime metadata; correctness comparison. Pig and Hive are future Phase 2 backends. The Hadoop backend uses local JobRunner for Phase 1 (MapReduce tasks still run), not a multi-node cluster.

## Prerequisites

- Docker Desktop (allocate at least 6 GB RAM; 8 GB preferred), running
- Python 3.10+ (`python3` and `pip`)
- The Hadoop service builds from Apache's multi-architecture Hadoop 3.5.0 image (GHCR). MongoDB/PostgreSQL images also support ARM64. If the registry pull fails, share its exact error.

## Commands from the repository root

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
./scripts/doctor.sh

docker compose up -d mongodb postgres
# Confirm both Docker services say 'healthy':
docker compose ps
python -m app.cli setup-db
python -m app.cli download

# Official raw NASA input only; default input order is Jul95 then Aug95.
python -m app.cli run --pipeline mongo --batch-size 100000
python -m app.cli report

# Start Hadoop after successful MongoDB verification.
docker compose --profile mapreduce up -d --build hadoop
python -m app.cli run --pipeline mapreduce --batch-size 100000
python -m app.cli compare
python -m app.cli report
```

**Important:** Use identical batch sizes for fair runs. The download command only decompresses the official archives; it does not clean, filter, convert or rewrite records. Unit tests have *synthetic* strings only; never use those as experiment data.

## Architecture and semantics

`app.cli` -> chosen backend -> PostgreSQL -> report/compare. `common.spec` is the contract and offline test oracle, **not a shared ETL engine**. MongoDB performs parsing and all three groupings via `$regexFind`, `$group`, `$addToSet`, `$sort` and `$limit`; Python inserts untouched raw lines with transport metadata only. Hadoop MapReduce uses `NLineInputFormat` input splits, mapper/reducer shuffle/sort and a second global reducer for Q2 top 20. All actual MapReduce record transformations run within Hadoop mapper/reducer tasks.

Parse fields: host, original timestamp with UTC offset, local log date, local log hour, HTTP method, resource path, protocol version (nullable), status code, bytes (missing/`-` -> 0). Malformed lines never contribute to Q1–Q3 and are counted. Resource paths exclude query strings. Error rate is **error requests / all valid requests in the hour** (0–1). Query 2 top 20 sorted by request count descending, resource path ascending to break ties.

Batching: batches are made separately within each official source log, in Jul95 then Aug95 order; no batch spans files. Batch IDs start at 1 across the experiment. All raw input lines, including malformed records, count toward batch size. The last partial batch in each file is a valid nonempty batch. MongoDB creates input batches while importing raw lines; Hadoop input splitting itself defines batches (`NLineInputFormat`). The loader assigns sequential metadata IDs from job-reported splits. Note: a run aggregate is associated with the **final batch ID** in reporting tables; it summarizes all batches, not only the final batch.

Runtime starts just before the backend's first raw input read and stops immediately after all aggregates are committed to PostgreSQL. The subsequent runtime-metadata update and rendering are excluded. Batch statistics use `total_records / nonempty_batch_count`.

## High-confidence verification gates

1. `pytest` checks exact parsing rules, malformed conditions, missing bytes, request path handling, and performs a small real mapper/reducer subprocess test.
2. `app.validation` rejects count mismatches, incomplete batch counts, nonconsecutive batch IDs, invalid errors and incorrect top-20 sorting before DB commit.
3. `app.cli compare` refuses to compare runs unless their input file name/size manifests and batch sizes match, then demands matching Q1/Q2/Q3 integer outputs. Status counters and parsed outputs are stored with run IDs.
4. Confirm both backends pass all tests against the full official NASA data before calling the project Phase 1 complete.

## Project structure

- `app/cli.py` — unified user entry point
- `app/data.py` — download and allowed gzip decompression
- `common/spec.py` — documented parsing oracle, not a production ETL executor
- `pipelines/mongo/backend.py` — MongoDB backend
- `pipelines/mapreduce/` — Hadoop streaming backend
- `app/store.py`, `sql/schema.sql` — PostgreSQL schema and reporting
- `app/validation.py` — validation before commit
- `tests/` — offline regression tests
- `docs/PHASE1_DESIGN.md` — design, assumptions, reviewer questions, Phase 2 expansion

### Remaining limitations

- Live local-Docker integration has been verified; portability and multi-node deployment are not yet established.
- Mongo regex parsing and the Python mapper share a carefully aligned contract, but an end-to-end differential test must confirm all official dataset edge cases.
- A metadata dataset manifest of filename + file size is checked, not a cryptographic hash; enhance this in final Phase 2.
- Large Hadoop jobs with one reducer per stage prioritize correctness and reproducibility, not peak throughput.
- The project statement does not provide a marking rubric and does not specify a required Hadoop cluster size.
