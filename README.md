# NASA HTTP Log Analytics — Multi-Pipeline ETL Framework

> **DAS 839 · NoSQL Systems · End-Semester Project**\
> **Phase 1 status:** Two execution backends implemented and verified on the complete official NASA July/August 1995 logs. **Phase 2 status:** Pig and Hive are planned, not yet implemented.

This repository is a comparative ETL (extract–transform–load) prototype. A user chooses an execution backend, the backend processes **the same unmodified NASA HTTP logs** and evaluates **the same three analytical queries**, and a shared controller loads aggregated results into PostgreSQL. A reporting command reads PostgreSQL—**not the pipeline's temporary files**—to show both results and execution metadata.

The purpose is not to claim that one technology is universally faster. It is to compare implementation approaches and observed runtimes fairly, while verifying that the query outputs are equivalent.

## At a glance

| Property | Phase 1 implementation |
|---|---|
| Implemented processing backends | MongoDB aggregation; Apache Hadoop Streaming MapReduce (local JobRunner) |
| Reserved for Phase 2 | Apache Pig; Apache Hive |
| Source data | Official `NASA_access_log_Jul95.gz` and `NASA_access_log_Aug95.gz` (decompressed only) |
| Data warehouse/reporting store | PostgreSQL 17 |
| Application entry point | `python -m app.cli` |
| Deployment | Python on host Mac + Docker Compose services |
| Workload | Daily traffic, top 20 requested paths, hourly HTTP error rates |
| Batching | Raw input record count, configured by `--batch-size`, sequential IDs |
| Verification | Automated tests, live database audit, cross-pipeline output comparison |

### Verified experimental results

Six complete runs (three per backend) used **3,461,613 raw input records** with batch size **100,000**. Each reported **3,930 malformed records**, **35 nonempty batches**, and exactly matching aggregated values in all three queries: Q1 **288** rows, Q2 **20** rows, and Q3 **1,369** rows.

| Backend | Measured runtimes (seconds) | Mean (seconds) |
|---|---|---:|
| MongoDB | 97.734, 80.298, 79.310 | 85.781 |
| Hadoop MapReduce | 30.855, 32.742, 30.689 | 31.429 |

These measurements were obtained on one developer Mac with local Docker. **Hadoop runs in local mode**, not a distributed HDFS/YARN cluster. Results do not establish general performance superiority. The audit and detailed benchmark are recorded in [`docs/PHASE1_RESULTS.md`](docs/PHASE1_RESULTS.md) and [`docs/PHASE1_BENCHMARK.md`](docs/PHASE1_BENCHMARK.md).

## How it works

```text
Official NASA July/August raw logs
              |
       CLI / controller
     (one backend per run)
              |
       +------+------+
       |             |
  MongoDB ETL     Hadoop Streaming
  parse + group   mapper + shuffle + reducers
       |             |
       +------+------+
              |
    Schema-aligned Q1, Q2, Q3 aggregates
              |
   Input/batch/query correctness checks
              |
    PostgreSQL result & metadata tables
              |
       Report / compare / audit
```

The controller chooses and invokes a backend; **it does not secretly perform the backend's ETL or analytical aggregation**. MongoDB parses and aggregates using database expressions. Hadoop reads raw records using `NLineInputFormat`, and the transformation and grouping logic executes in mapper/reducer tasks. Common Python code defines the reference semantics and stores results, but does not replace a selected execution backend.

The architecture reserves adapters for Pig and Hive. They are explicitly unavailable until Phase 2; no results are represented as having been produced by those systems.

## Requirements

- macOS with Docker Desktop running (the code also has portable components, but full integration has only been verified on the developer Mac).
- Python 3.10 or newer; project-specific virtual environment recommended.
- Docker resources sufficient to run MongoDB, PostgreSQL, and the local Hadoop job. On the tested Mac, Docker had approximately 8 GB RAM.
- Internet connection for the initial official dataset and Docker image downloads.
- Available host ports `127.0.0.1:27018` (MongoDB) and `127.0.0.1:5437` (PostgreSQL).

Default PostgreSQL credentials in `compose.yaml` are **development-only**; do not expose these services to the public Internet or reuse these credentials for production. Docker's port mappings bind to loopback.

## Reproduce Phase 1 from a fresh clone

Open Terminal in the repository directory. Execute commands in the displayed order. Lines beginning with `#` are descriptive comments; paste executable lines separately if your interactive Zsh setup does not support comments.

**1. Configure Python and run offline tests**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
./scripts/doctor.sh
```

**2. Start the required databases**

```bash
docker compose up -d --wait mongodb postgres
docker compose ps
python -m app.cli setup-db
```

`setup-db` creates tables without destroying existing project results. It requires a healthy PostgreSQL service. Never use `docker compose down -v` on a database you wish to preserve: `-v` deletes the named database volumes.

**3. Download the official input**

```bash
python -m app.cli download
```

This downloads **only** the official July and August gzip archives and decompresses them to `data/`. No external cleaning, record removal, CSV/JSON conversion, or synthetic data is used for experimental runs. Files in `data/` are deliberately excluded from Git because they are large and independently downloadable.

**4. Run MongoDB and show stored results**

```bash
python -m app.cli run --pipeline mongo --batch-size 100000
python -m app.cli report
```

**5. Start Hadoop and run MapReduce**

```bash
docker compose --profile mapreduce up -d --build hadoop
python -m app.cli run --pipeline mapreduce --batch-size 100000
python -m app.cli compare
python -m scripts.phase1_audit
```

The Hadoop service is controlled from Terminal. Do not separately click **Run** in Docker Desktop. The Hadoop image build/download is a one-time setup cost, excluded from ETL runtime.

The comparison must confirm identical Q1, Q2, and Q3 aggregated values. The audit independently checks all stored batch rows, input file fingerprints, record totals, and SQL result metadata. The audit writes the evidence document to `docs/PHASE1_RESULTS.md` but does not modify PostgreSQL or the original logs.

**6. Repeat experiments if needed**

```bash
./scripts/run_phase1_trials.sh
```

This runs four additional experiments (MapReduce, MongoDB, MongoDB, MapReduce) and updates the benchmark report. It is not necessary for everyday development, and will write additional successful runs to PostgreSQL.

## Three mandatory queries

**Q1 — Daily Traffic Summary.** For each local log date and HTTP status code: `log_date`, `status_code`, `request_count`, `total_bytes`.

**Q2 — Top Requested Resources.** Globally top 20 paths by request count: `resource_path`, `request_count`, `total_bytes`, `distinct_host_count`. Ties use ascending resource path for deterministic ranking.

**Q3 — Hourly Error Analysis.** For every local log date/hour: `log_date`, `log_hour`, `error_request_count` (status 400–599), `total_request_count`, `error_rate` (errors divided by valid requests in that hour), `distinct_error_hosts`.

Data preparation rules and documented edge cases are in [`docs/PHASE1_DATA_QUALITY.md`](docs/PHASE1_DATA_QUALITY.md). All raw input records—including malformed ones—count toward batch sizes. Malformed lines are reported, not silently dropped. A missing or `-` bytes value is treated as zero. The `timestamp`, `host`, `http_method`, `resource_path`, and `protocol_version` fields are extracted as part of each backend's transformation logic even when not present in the final aggregate schema.

## Code and documentation map

| Location | Purpose |
|---|---|
| `app/cli.py` | CLI arguments, backend selection, run ID, runtime orchestration |
| `app/data.py` | Official archive download and allowed decompression |
| `app/validation.py` | Structural and arithmetic checks before SQL persistence |
| `app/store.py` | PostgreSQL writing, report reading and comparison |
| `common/spec.py` | Canonical parsing rules and offline reference oracle |
| `pipelines/mongo/backend.py` | MongoDB raw ingestion and aggregation pipelines |
| `pipelines/mapreduce/` | Hadoop launcher plus mapper/reducer programs |
| `sql/schema.sql` | Non-destructive SQL schema creation |
| `tests/` | Unit and regression tests, including Hadoop worker protocol tests |
| `scripts/phase1_audit.py` | Read-only SQL+data audit, writes reproducible evidence Markdown |
| `scripts/run_phase1_trials.sh` | Repeat full-data experiment runs and generate statistics |
| `docs/PHASE1_DESIGN.md` | Full architecture, design tradeoffs and specification mapping |
| `docs/PHASE1_RESULTS.md` | Latest verified run IDs, checksums and stored result counts |
| `docs/PHASE1_BENCHMARK.md` | Multi-run timing statistics |
| `docs/GIT_WORKFLOW.md` | Local, testing, commit and GitHub push workflow |

`data/`, `work/`, `.venv/`, and `__pycache__/` are local or generated and must not be committed. The project deliberately stores its results in Docker-managed PostgreSQL volumes, not in the Git repository.

## Limitations and next phase

Phase 1 demonstrates the required architecture and two processing paradigms. It is **not** the four-backend final system. Pig and Hive remain Phase 2 tasks, and Hadoop runs with the local JobRunner instead of a distributed cluster.

There are three documented design interpretations for the instructor: batches reset at file boundaries while IDs continue globally; whole-run aggregates carry the final batch ID; and resource paths strip URL query strings. See [`docs/PHASE1_DESIGN.md`](docs/PHASE1_DESIGN.md) for justification and remaining risks.

**A passing comparison proves equivalence between these two implementations under the shared rules; it does not prove that every uncommon NASA HTTP record was interpreted exactly as the instructor intends.** All edge-case classifications and the extra eight-byte unterminated July fragment are documented for review.

### Next Phase 2 additions

Pig and Hive adapters must apply the same parsing/cleaning/aggregation semantics using actual Pig Latin and HiveQL, then return the same Q1/Q2/Q3 structures through the existing controller and PostgreSQL loader. Their implementation must not be substituted with MongoDB or Python-only ETL.

For full engineering design details, read [`docs/PHASE1_DESIGN.md`](docs/PHASE1_DESIGN.md). For the Git workflow, read [`docs/GIT_WORKFLOW.md`](docs/GIT_WORKFLOW.md).
