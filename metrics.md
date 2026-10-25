# Pipeline Metrics Log

Running record of measured numbers from each milestone.
Source of truth for README claims and resume bullets — do not quote a number that isn't in here.

---

## Environment

| Item | Value |
|---|---|
| Platform | Databricks Free Edition (serverless compute only) |
| Runtime | Databricks Runtime 19.11.x, Photon, aarch64 |
| Catalog | `workspace` |
| Schemas | `bronze`, `silver`, `gold` |
| Dataset | Credit Card Transactions Fraud Detection (Sparkov / kartik2112), `fraudTrain.csv` |
| Licence | CC0: Public Domain |
| Coverage | 1 Jan 2019 – 31 Dec 2020 · 1,000 cards · 800 merchants |

### Dataset switch

Initially ingested the ULB `creditcard.csv` (284,807 rows, 31 cols). Replaced with Sparkov because
ULB's features are pre-PCA'd (`V1`–`V28`) with no customer key, no merchant and no real timestamp —
which left nothing to engineer in the gold layer and made window functions impossible.

**The switch required changing one line** (`RAW_FILE`). The bronze layer absorbed a completely
different schema without any change to ingestion logic, because it lands every column as STRING
and infers nothing. This is the payoff of the untyped-bronze decision.

---

## Milestone 1 — Bronze ingestion

**Date:** 2026-10-25
**Table:** `workspace.bronze.transactions_raw`

| Metric | Value | How measured |
|---|---|---|
| Source rows | **1,296,675** | `df.count()` |
| Source columns | 23 | 26 total minus 3 pipeline metadata cols |
| Total columns written | 26 | `len(bronze.columns)` |
| Source CSV size | 335 MB (343,007 KB) | Windows file properties |
| Delta table size | 65.6 MB (68,742,869 bytes) | `DESCRIBE DETAIL` → `sizeInBytes` |
| **Storage reduction** | **~80%** | CSV vs Delta |
| Parquet files written | 6 | `DESCRIBE DETAIL` → `numFiles` |
| Compression codec | zstd | `DESCRIBE DETAIL` → `properties` |
| Ingest runtime (warm) | **24.7 s** | wall clock in notebook |
| Delta version | 0 | `DESCRIBE HISTORY` |
| Delta operation | `CREATE OR REPLACE TABLE AS SELECT` | `DESCRIBE HISTORY` |
| Batch ID | `b6babf9c-1235-4f53-a2eb-8e82427a3f66` | generated per run |

### Runtime comparison (worth quoting)

| Run | Rows | CSV size | Runtime |
|---|---|---|---|
| ULB (cold start) | 284,807 | 144 MB | 50.8 s |
| Sparkov (warm) | 1,296,675 | 335 MB | **24.7 s** |

4.5× the rows and 2.3× the file size, in **half the time** — because the first run paid serverless
cold-start cost and the second did not. Neither figure is steady-state throughput. The lesson is
that a single wall-clock number means nothing without knowing whether compute was warm.

### Source columns

`_c0` (row index artifact), `trans_date_trans_time`, `cc_num`, `merchant`, `category`, `amt`,
`first`, `last`, `gender`, `street`, `city`, `state`, `zip`, `lat`, `long`, `city_pop`, `job`,
`dob`, `trans_num`, `unix_time`, `merch_lat`, `merch_long`, `is_fraud`

Plus pipeline metadata: `_ingest_ts`, `_source_file`, `_batch_id`

### Design notes (for interviews)

- Bronze lands **all columns as STRING**, `inferSchema=false`. Avoids a second full scan of the
  file, and avoids non-deterministic type inference across batches (a column that's all digits
  today infers as int; one bad value tomorrow infers as string and breaks downstream).
  Casting is deferred to silver where it is explicit, reviewable and testable.
- Three lineage columns: `_ingest_ts`, `_source_file` (via Spark `_metadata.file_path`),
  `_batch_id`. Enables per-batch rollback instead of full rebuild.
- Ingestion is a manual upload to a Unity Catalog volume, because Free Edition restricts outbound
  internet from compute to a trusted-domain allowlist. In production this would be a cloud landing
  zone read incrementally with Auto Loader.
- `_c0` is an unnamed row-index column in the source CSV. Left intact in bronze (bronze preserves
  the source as-is); dropped in silver.
- **PII present in bronze:** `cc_num`, `first`, `last`, `street`, `dob`. Masked at the silver
  boundary — see Milestone 2.

---

## Milestone 2 — Silver

*Pending.*

**Tables:** `workspace.silver.transactions_clean`, `workspace.silver.transactions_quarantine`

| Metric | Value |
|---|---|
| Rows in | |
| Quarantined (count) | |
| Quarantined (%) | |
| Duplicate `trans_num` removed | |
| Rows out | |
| Fraud rows | |
| **Fraud rate** | |
| Runtime | |
| Silver `sizeInBytes` | |

**Design notes to record:**
- Quality failures are **quarantined with a failure reason**, not filtered away (dead-letter
  queue pattern). Silently dropping rows makes data-quality problems invisible.
- PII masked at the silver boundary: `cc_num` → SHA-256 `card_id` (still a valid grouping key for
  window functions), `dob` → `customer_age`, names and street dropped.
  Known limitation: unsalted hash is vulnerable to a guess-and-confirm attack; real tokenization
  uses a salt or a vault.
- `.cache()` on the flagged DataFrame — Spark is lazy, so without it the full 1.3M-row cast and
  rule evaluation is recomputed once per downstream action.

---

## Milestone 3 — Gold

*Pending. Feature table + aggregate table. Record feature count, window spec, runtime, row count.*

Planned features using `card_id` as partition key: rolling spend per card, time since that card's
previous transaction, amount vs that card's running average, haversine distance between cardholder
(`lat`/`long`) and merchant (`merch_lat`/`merch_long`), hour-of-day and day-of-week.

---

## Milestone 4 — Model

*Pending. Record PR-AUC, ROC-AUC, precision, recall, F1 at chosen threshold, and class balance.
**Not accuracy** — at ~0.5% fraud, predicting "never fraud" scores ~99.5% accuracy and is useless.*

| Metric | Value |
|---|---|
| Train/test split basis | chronological (no future→past leakage) |
| Fraud rate in training data | |
| PR-AUC | |
| ROC-AUC | |
| Precision @ threshold | |
| Recall @ threshold | |
| Threshold chosen | |

---

## Milestone 7 — Orchestration

*Pending. Record task count, schedule, median end-to-end runtime, consecutive successful runs.*
