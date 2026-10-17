# Pipeline Metrics Log

Running record of measured numbers from each milestone.
Source of truth for README claims and resume bullets — do not quote a number that isn't in here.

---

## Environment

| Item | Value |
|---|---|
| Platform | Databricks Free Edition (serverless compute only) |
| Catalog | `workspace` |
| Schemas | `bronze`, `silver`, `gold` |
| Dataset | Credit Card Fraud Detection (ULB) — `creditcard.csv` |

---

## Milestone 1 — Bronze ingestion

**Date:** 2026-10-17
**Table:** `workspace.bronze.transactions_raw`

| Metric | Value | How measured |
|---|---|---|
| Source rows | 284,807 | `df.count()` / `SELECT COUNT(*)` |
| Source columns | 31 | 34 total minus 3 pipeline metadata cols |
| Total columns written | 34 | `len(bronze.columns)` |
| Source CSV size | ~144 MB | *(verify in Catalog → Volumes)* |
| Delta table size | 67.9 MB (71,190,479 bytes) | `DESCRIBE DETAIL` → `sizeInBytes` |
| **Storage reduction** | **~53%** | CSV size vs Delta size |
| Parquet files written | 3 | `DESCRIBE DETAIL` → `numFiles` |
| Compression codec | zstd | `DESCRIBE DETAIL` → `properties` |
| Ingest runtime | 50.8 s | wall clock in notebook (incl. cold start) |
| Delta version | 0 | `DESCRIBE HISTORY` |
| Delta operation | `CREATE OR REPLACE TABLE AS SELECT` | `DESCRIBE HISTORY` |
| Batch ID | `6d1e6327-3cfb-4712-bc48-c50d84cbff07` | generated per run |

**Design notes (for interviews):**
- Bronze lands **all columns as STRING**, `inferSchema=false`. Avoids a second full scan of the file, and avoids non-deterministic type inference across batches. Casting is deferred to silver where it is explicit and testable.
- Three lineage columns added: `_ingest_ts`, `_source_file` (via Spark `_metadata.file_path`), `_batch_id`. Enables per-batch rollback instead of full rebuild.
- Ingestion is a manual upload to a Unity Catalog volume, because Free Edition restricts outbound internet from compute to a trusted-domain allowlist. In production this would be a cloud landing zone read incrementally with Auto Loader.
- First cell ran 23 s and ingestion 50.8 s — both include serverless cold start, so these are not steady-state throughput figures.

---

## Milestone 2 — Silver

*Pending.*

| Metric | Value |
|---|---|
| Rows in | |
| Exact duplicate rows removed | |
| Rows failing quality checks | |
| Rows out | |
| Runtime | |

---

## Milestone 3 — Gold

*Pending.*

---

## Milestone 4 — Model

*Pending. Record PR-AUC, ROC-AUC, precision, recall, F1 at chosen threshold, and the class balance. Not accuracy.*

| Metric | Value |
|---|---|
| Fraud rate in training data | |
| PR-AUC | |
| ROC-AUC | |
| Precision @ threshold | |
| Recall @ threshold | |

---

## Milestone 7 — Orchestration

*Pending. Record task count, schedule, median end-to-end runtime, consecutive successful runs.*
