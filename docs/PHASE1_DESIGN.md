# Phase 1 — Architecture, Decisions, and Specification Traceability

## 1. Purpose and project scope

The DAS 839 end-semester assignment is a **multi-pipeline ETL and reporting framework** for the official NASA HTTP web-server logs. The final system must let a user choose among **Apache Pig, Hadoop MapReduce, MongoDB and Hive** while preserving identical input and query semantics. It must store aggregated results in a relational database and report runtime and batching metadata.

**Phase 1**, unlike Phase 2, calls for a proposed overall architecture and a working prototype with **at least one or two pipelines**. This repository implements **MongoDB and Hadoop MapReduce** in Phase 1. The remaining two backends are architectural placeholders only, not working executors.

The prototype has passed full-dataset execution, PostgreSQL reporting, three-query equivalence, and 19 offline tests in the developer's recorded environment. Exact evidence is in `PHASE1_RESULTS.md` and `PHASE1_BENCHMARK.md`. Those artifacts are observations, not guaranteed results on other computers.

## 2. System components

```text
                         +--------------------+
                         |  User / Python CLI |
                         +----------+---------+
                                    |
                 backend choice + raw files + batch size + run ID
                                    |
                         +----------v---------+
                         | Controller app/cli  |
                         +----------+---------+
                                    |
                      +-------------+-------------+
                      |                           |
            +---------v---------+       +---------v----------+
            | MongoDB executor  |       | Hadoop MapReduce   |
            | database parsing  |       | Streaming tasks    |
            | $group, etc.      |       | NLineInputFormat   |
            +---------+---------+       +---------+----------+
                      |                           |
                      +-------------+-------------+
                                    |
                         canonical results object
                                    |
                 +------------------v------------------+
                 | Validator -> PostgreSQL load/commit  |
                 +------------------+------------------+
                                    |
                  +-----------------v-----------------+
                  | SQL-backed report and comparison   |
                  +-----------------------------------+

            Phase 2 reserved adapters: Apache Pig, Hive
```

**Boundary rule:** the controller is allowed to select a backend, pass original paths and a batch size, collect results, validate metadata, and load into SQL. It must not parse or aggregate raw logs on the backend's behalf. `common/spec.py` defines reference rules for tests; the Hadoop mapper is an actual Hadoop task and may import it **inside that task**, not as a controller preprocessing step.

## 3. Data source and preparation rules

Official sources: [NASA HTTP dataset description](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html), and the July and August 1995 trace archives from the Internet Traffic Archive.

1. The download module fetches the official `.gz` archives.
2. Decompression is allowed; no cleaning, filtering, JSON/CSV conversion, or record removal occurs outside the chosen pipeline.
3. Both backends read the same July and August files in the same order.
4. The experimental batch size is **100,000 raw input records**; malformed lines count toward batch size.
5. All input-line counts, fingerprints and final-newline behavior are recorded by the quality audit.

The July file contains an additional **eight-byte incomplete trailing fragment**, so counting physical input records yields **3,461,613**, compared with 3,461,612 newline-terminated records. The fragment is counted as malformed; it is not edited out. See `PHASE1_DATA_QUALITY.md`.

### Shared structured fields

| Field | Semantics |
|---|---|
| `host` | Original client host from NASA log |
| `timestamp` | Original timestamp including timezone offset |
| `log_date` | Calendar date in the log's recorded clock time |
| `log_hour` | Hour 0–23 in the recorded clock time |
| `http_method` | Method extracted from request, if valid |
| `resource_path` | Request path before the first `?`, case preserved |
| `protocol_version` | HTTP protocol token if present; nullable |
| `status_code` | Three-digit HTTP response code |
| `bytes_transferred` | Numeric bytes; missing or `-` becomes zero |

A record failing shared parsing rules does not enter the analytical aggregates. It is counted as malformed, including malformed request strings and malformed outer log structures. Under the current contract, 3,897 records fail request parsing and 33 fail outer parsing: **3,930 malformed records** total. Both production backends agree on these totals for the official data.

## 4. Backend execution semantics

### MongoDB

- The host-side Python executor sends unmodified raw lines, transport metadata and assigned batch IDs to temporary MongoDB collections.
- MongoDB aggregation expressions parse raw strings, derive the structured fields, and identify malformed records.
- `$group`, `$addToSet`, `$sort`, and `$limit` perform the three analytical queries.
- Temporary per-run collections are removed after execution, including on errors; aggregate results are passed to the SQL loader.
- The controller does not calculate query results in Python.

### Hadoop MapReduce

- Hadoop Streaming runs real Python mapper/reducer executables under **local JobRunner**; it is not a multi-node Hadoop deployment.
- `NLineInputFormat` creates input splits sized by record count, so batching happens inside the selected processing backend.
- Each mapper parses input records and emits tagged key-value records for Q1, Q2, Q3, malformed tracking and batch accounting.
- Hadoop performs map/reduce shuffle and sorting. The first reducer aggregates daily, resource and hourly measures, including distinct host sets.
- A second Hadoop Streaming job ranks Q2's global top 20 resource paths.
- The driver converts finished Hadoop output into the same canonical result dictionaries used by the SQL loader. The driver does not substitute its own parsing or grouping logic.

A Hadoop Streaming mapper may receive an input offset key followed by a tab and the original line. The implemented mapper handles this transport format. Source NASA bytes are decoded using Latin-1 to preserve byte values without Unicode decoding exceptions. Both behavior choices have regression tests.

### Why compare these two?

They represent distinct processing paradigms: MongoDB's document-oriented aggregation operators versus Hadoop's explicit mapper/shuffle/reducer task model. This contrast is useful even in single-machine prototype mode. Reported performance differences are **specific to this execution setup**, and are not claims about optimal distributed deployments.

## 5. Mandatory queries and output contract

**Q1 — Daily Traffic Summary:** group valid requests by `log_date, status_code`, return total `request_count` and `total_bytes`.

**Q2 — Top Requested Resources:** group by `resource_path`, count requests, sum bytes, count unique requesting hosts, sort by `request_count DESC, resource_path ASC`, then retain exactly the global top 20 where available.

**Q3 — Hourly Error Analysis:** group valid requests by `log_date, log_hour`, count all requests and 400–599 error requests, compute `error_rate = error_request_count / total_request_count` on the 0–1 scale, and count distinct hosts among error requests only.

The relational loader persists all required logical columns plus `run_id`, `pipeline`, `batch_id` and `executed_at`. Query order and tie-breaking are deterministic for fair comparisons.

## 6. Batching design

A **batch is a group of raw input records**, not a number of bytes or a number of valid parsed requests. IDs start at 1 and increment sequentially over the complete run.

For the tested batch size of 100,000:

| Source | Raw records | Nonempty batches | Final batch size |
|---|---:|---:|---:|
| July | 1,891,715 | 19 | 91,715 |
| August | 1,569,898 | 16 | 69,898 |
| **Total** | **3,461,613** | **35** | — |

Batches reset at each source-file boundary while IDs remain globally sequential. Therefore, there are two partial batches, and the average is **3,461,613 ÷ 35 = 98,903.2286** records per nonempty batch. The audit verifies each stored `batches` row against the original source-file record counts.

**Open interpretation:** the assignment does not expressly resolve whether a batch may span two files. This design chooses source-file-local batching for source traceability. If required by the instructor, adjust *both* backends and rerun equivalence experiments.

## 7. Relational database model

The schema is created by `sql/schema.sql`; it uses `CREATE TABLE IF NOT EXISTS` rather than dropping existing data.

| Table | Role | Core columns |
|---|---|---|
| `runs` | One row per submitted ETL run | run ID, pipeline, file manifest, configured batch size, batch count, totals, malformed, average, timing and status |
| `batches` | One row per nonempty batch | run ID, sequential batch ID, source file, count |
| `q1_daily_traffic` | Q1 aggregated results | date, status, request count, bytes + execution metadata |
| `q2_top_resources` | Q2 aggregated results | path, requests, bytes, distinct hosts + execution metadata |
| `q3_hourly_errors` | Q3 aggregated results | date, hour, error count, total count, error rate, distinct error hosts + execution metadata |

The transaction writes all query results and batch metadata together to PostgreSQL, then records final runtime and status. Failed operations must not be presented as verified analytical successes. The validator rejects empty/all-malformed results for the official project workload and checks basic accounting invariants before the SQL transaction.

**Aggregate batch-ID convention:** result rows represent the **full run**, but carry the last batch's ID as execution metadata. The `batches` table contains the actual per-batch counts. This convention should be explained explicitly during review; it does not mean that a Q1/Q2/Q3 row describes only the final batch.

## 8. Runtime measurement and fairness

Runtime is measured from just before selected ETL execution starts (prior to backend raw reads) through the point where all aggregated rows have committed successfully to PostgreSQL. The separate rendering of the SQL report, input download and image/software installation are excluded. Final runtime-metadata update occurs after the timer stops and is a documented boundary.

Fair-comparison controls:

- Same two official raw files, unchanged, in July–August order.
- Same shared parsing, cleaning, filtering and grouping rules.
- Same configured batch size (100,000) for all recorded runs.
- Same Q1/Q2/Q3 output definitions and deterministic Q2 tie handling.
- Exact SQL comparison of all query values, including stored hourly error rates, through the full audit.
- Six observed runs, including repeat experiments, rather than relying on one timing.

Three MongoDB runtimes: **97.734, 80.298, 79.310 seconds** (mean **85.781**). Three MapReduce runtimes: **30.855, 32.742, 30.689 seconds** (mean **31.429**). MapReduce's local measured mean was approximately **2.73× faster** in these runs. Startup/cache effects, different internal workload implementations and the local execution environment limit generalization.

## 9. Test, reconciliation and evidence strategy

**Offline regression tests (`python -m pytest -q`)** check parsing edge cases, malformed and missing bytes, time/date derivation, top ranking, Hadoop worker encoding and Streaming key/value handling, and audit invariants. The latest reported count is **19 passed**.

**End-to-end verification (`python -m scripts.phase1_audit`)** hashes official inputs and inspects *stored* SQL metadata, every batch, sum-of-requests invariants, all three query output tables, and equality between the latest two successful backends. It regenerates `PHASE1_RESULTS.md` only on a passing audit. This is stronger than merely comparing row counts.

**Known limitations:** run manifests contain filename and byte length, not execution-time SHA-256; the audit hashes current local files. The two-backend equality test cannot independently prove every unusual HTTP request matches the instructor's intended interpretation. Resource-path normalization and strict protocol parsing should be confirmed when reviewing edge cases.

## 10. Phase 1 requirements coverage

| Requirement | Evidence / current state |
|---|---|
| Architecture for four selectable paradigms | Unified CLI reserves all four options; Pig/Hive adapters planned |
| Working 1–2 pipelines | MongoDB and MapReduce full input runs passed |
| Official NASA raw logs | Only July and August decompressed traces used |
| Parsing, cleaning, malformed accounting | Shared contract; 3,930 malformed, independently audited |
| All three mandatory queries | Q1 288, Q2 20, Q3 1,369, exact cross-backend values |
| Record-count batching | 35 sequential, nonempty batches, partial batches included |
| Relational aggregated results | PostgreSQL tables with run/pipeline/batch/execution metadata |
| Report and runtime | SQL-backed report and six measured runtimes |
| Equivalence plan | Canonical test oracle, live SQL comparison, audit scripts |
| Full four-pipeline delivery | **Phase 2, not claimed complete** |

## 11. Remaining decisions and Phase 2 plan

1. Confirm that Hadoop local JobRunner is sufficient for the Phase 1 demonstration. Real MapReduce jobs run, but no HDFS/YARN cluster is used.
2. Confirm batch resets per file are acceptable; changing that must apply to all backends.
3. Confirm source request targets should be normalized by removing query strings.
4. Confirm the aggregate result `batch_id` convention for all-run results.
5. Decide whether unusual protocol tokens (for example nonstandard HTTP-version strings) must count as malformed, while keeping equivalent behavior across all backends.

For Phase 2, add real Apache Pig and Hive code paths under `pipelines/`, not Python/MongoDB substitutes. Each must accept official raw logs and return the same canonical result schema. The controller, PostgreSQL schema and reporting layer are designed to be reused. Do not claim unimplemented adapters are complete.

## 12. Evidence references and maintenance

- `PHASE1_RESULTS.md` — audit-generated latest runs, SHA-256 hashes, batching and query counts.
- `PHASE1_BENCHMARK.md` — six-run runtime statistics, created by the benchmark script.
- `PHASE1_DATA_QUALITY.md` — raw-record and malformed policy, including July end-of-file fragment.
- `../README.md` — fresh-clone setup, commands and navigation.
- `GIT_WORKFLOW.md` — review, commit and push process; safe repository hygiene.

The report may be presented as evidence of completed work. The current phase does not require inventing team contributions or attribution that cannot be substantiated.
