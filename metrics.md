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
| Coverage | `fraudTrain.csv` spans Jan 2019 – late Jun 2020 (537 days) · 1,000 cards · 800 merchants |

### Dataset switch

Initially ingested the ULB `creditcard.csv` (284,807 rows, 31 cols). Replaced with Sparkov because
ULB's features are pre-PCA'd (`V1`–`V28`) with no customer key, no merchant and no real timestamp —
which left nothing to engineer in the gold layer and made window functions impossible.

**The switch required changing one line** (`RAW_FILE`). The bronze layer absorbed a completely
different schema without any change to ingestion logic, because it lands every column as STRING
and infers nothing. This is the payoff of the untyped-bronze decision.

---

## Milestone 1 — Bronze ingestion

**Table:** `workspace.bronze.transactions_raw`

| Metric | Value |
|---|---|
| Source rows | **1,296,675** |
| Source columns | 23 (+3 pipeline metadata = 26 written) |
| Source CSV size | 335 MB |
| Delta table size | 65.6 MB (68,742,869 bytes) |
| **Storage reduction** | **~80%** |
| Parquet files written | 6 |
| Compression codec | zstd |
| Ingest runtime (warm) | **24.7 s** |
| Delta version / operation | 0 / `CREATE OR REPLACE TABLE AS SELECT` |

### Runtime comparison

| Run | Rows | CSV size | Runtime |
|---|---|---|---|
| ULB (cold start) | 284,807 | 144 MB | 50.8 s |
| Sparkov (warm) | 1,296,675 | 335 MB | **24.7 s** |

4.5× the rows in half the time — the first run paid serverless cold-start cost. Neither figure is
steady-state throughput. A single wall-clock number means nothing without knowing whether compute
was warm.

### Design notes

- Bronze lands **all columns as STRING**, `inferSchema=false`. Avoids a second full scan of the
  file, and avoids non-deterministic type inference across batches. Casting is deferred to silver
  where it is explicit, reviewable and testable.
- Three lineage columns: `_ingest_ts`, `_source_file` (via Spark `_metadata.file_path`),
  `_batch_id`. Enables per-batch rollback instead of full rebuild.
- Manual upload to a Unity Catalog volume, because Free Edition restricts outbound internet from
  compute to a trusted-domain allowlist. In production: a cloud landing zone read with Auto Loader.
- **PII present in bronze:** `cc_num`, `first`, `last`, `street`, `dob`. Masked at silver.

---

## Milestone 2 — Silver

**Tables:** `workspace.silver.transactions_clean`, `workspace.silver.transactions_quarantine`

| Metric | Value |
|---|---|
| Rows in | 1,296,675 |
| Quarantined | 0 (0.0000%) |
| Duplicate `trans_num` removed | 0 |
| Rows out | 1,296,675 |
| Fraud rows | 7,506 |
| **Fraud rate** | **0.5789%** (172:1 imbalance) |
| Runtime (standalone notebook) | 76.8 s |
| Runtime (as job task) | 29 s |

The standalone notebook triggered six separate Spark actions for reporting; the job-task version
computes far fewer. Same logic, different instrumentation.

### Design notes

- Quality failures are **quarantined with a failure reason**, not filtered away (dead-letter queue
  pattern). Silently dropping rows makes data-quality problems invisible.
- Zero quarantined rows is expected — Sparkov is synthetic and clean. The mechanism is the
  deliverable, not the finding. Unit tests prove the rules fire on deliberately malformed rows.
- PII masked at the silver boundary: `cc_num` → SHA-256 `card_id` (still a valid grouping key for
  window functions), `dob` → `customer_age`, names and street dropped.
  **Known limitation:** unsalted hash is vulnerable to guess-and-confirm. Real tokenization uses a
  salt or a vault.
- `.cache()` on the flagged DataFrame — Spark is lazy, so without it the full 1.3M-row cast and
  rule evaluation is recomputed once per downstream action.

---

## Milestone 3 — Gold

**Tables:** `workspace.gold.transaction_features`, `workspace.gold.daily_fraud_summary`

| Metric | Value |
|---|---|
| Feature rows | 1,296,675 |
| Feature columns | 34 |
| Daily summary rows | 537 |
| Runtime (standalone) | 46.8 s |
| Runtime (as job task) | 17 s |

### Feature separation (evidence the engineering did something)

| `amt_vs_card_avg` | Value |
|---|---|
| Mean for fraudulent transactions | **6.49** |
| Mean for legitimate transactions | **0.968** |

Fraudulent transactions average 6.5× the cardholder's own running average. Legitimate ones sit at
it. That separation comes from a hand-built window feature, independent of any model.

### Window specs

All partitioned by `card_id`, ordered by `unix_time`, **all ending at the current row**:

- `running` — `rowsBetween(unboundedPreceding, 0)` → expanding average
- `24h` — `rangeBetween(-86400, 0)` → true rolling 24 hours
- `7d` — `rangeBetween(-604800, 0)`
- `lag` — unbounded, for `row_number()` and `lag()`

`rowsBetween` counts ROWS; `rangeBetween` counts VALUES of the ordering column. Since `unix_time`
is in seconds, `-86400` is a genuine 24-hour window regardless of transaction count. **No window
uses `unboundedFollowing`** — a feature built from future rows would not exist at scoring time.

---

## Milestone 4 — Model

| Metric | Value |
|---|---|
| Split basis | chronological, 80th percentile of `txn_date` |
| Split date | 2020-03-06 |
| Train rows | 1,036,895 (5,968 fraud) |
| Test rows | 259,780 (1,538 fraud) |
| Class imbalance | 172.7 : 1 |
| **PR-AUC** | **0.9699** |
| ROC-AUC | 0.9995 |
| Precision @ 0.5 | 0.6542 |
| Recall @ 0.5 | 0.9766 |
| F1 | 0.7835 |
| Confusion | caught 1,502 · missed 36 · false alarms 794 |
| Runtime | 28.9 s |

### Top features by importance

| Feature | Importance |
|---|---|
| `amt_sum_24h` | 0.3087 |
| `cat_gas_transport` | 0.1133 |
| `amt` | 0.0938 |
| `cat_grocery_pos` | 0.0616 |
| `cat_grocery_net` | 0.0536 |

### The caveat that must be volunteered

PR-AUC of 0.97 is **far above** what real card-fraud models achieve (~0.3–0.6). The reason is the
data: Sparkov is synthetic, and the simulator generates fraud using explicit rules — bursts of
transactions, inflated amounts, particular categories. Those patterns are far more learnable than
real fraud.

The pipeline was checked for leakage: every window ends at the current row, the split is
chronological, no feature uses future information. The number is real for this dataset; it is not
a claim about real-world fraud detection.

**Accuracy is deliberately not reported.** At 0.58% fraud, predicting "never fraud" scores 99.42%
accuracy and catches nothing.

Precision 0.65 means roughly 1 in 3 alerts is a false positive — 794 of them. At a real bank that
is an analyst-hours cost, and the threshold is where that trade-off gets negotiated.

---

## Milestone 5 — Tested package

| Metric | Value |
|---|---|
| Unit tests | **27** |
| Suite runtime (Docker) | ~55 s |
| Real defects caught pre-ship | 2 |

**The two defects:**
1. `QUALITY_RULES` was a module-level dict of `F.col(...)` expressions. Building a Column requires a
   live SparkContext. Worked in Databricks (always has a session), crashed on import in tests.
   Fixed by constructing the rules inside a function.
2. Test fixtures relied on Spark's type inference, which cannot infer a type for an all-null column.
   Fixed with an explicit all-STRING schema — which is also more faithful to what bronze actually is.

**Containerized test suite:** PySpark's Python workers do not run reliably on Windows without a
Hadoop `winutils` install. The suite runs in a Linux container (`Dockerfile.test`), matching the
GitHub Actions runner, so a local pass means a CI pass.

---

## Milestone 6 — CI/CD

| Metric | Value |
|---|---|
| CI runtime (passing) | 1 m 30 s |
| CI runtime (failing) | 1 m 4 s |
| Branch protection | `main` requires a passing `pytest` check + a pull request |

**Verified by deliberate failure.** A branch changed the 24-hour window from
`rangeBetween(-86400, 0)` to `rowsBetween(-3, 0)` — a change that reads as a harmless
simplification and would plausibly pass review. CI failed on
`test_24h_window_is_time_based_not_row_based` with `assert 4 == 1`, and branch protection blocked
the merge.

That test is a trap set for one specific future mistake. It is the most useful thing in the repo.

---

## Milestone 7 — Orchestration

| Metric | Value |
|---|---|
| Tasks | 4, chained by dependency |
| End-to-end runtime | **2 m 40 s** |
| Schedule | daily, 04:30 America/Toronto |
| Consecutive successful scheduled runs | 4 (Nov 11–14) + 2 manual |
| Runtime spread | 2 m 34 s – 2 m 43 s |

| Task | Runtime |
|---|---|
| `bronze_ingest` | 1 m 0 s |
| `silver_transform` | 29 s |
| `gold_features` | 17 s |
| `train_model` | 51 s |

**The job sources notebooks from GitHub `main`, not from the workspace.** Combined with branch
protection, that closes the loop: code only reaches `main` through a PR with passing tests, and the
scheduled job runs exactly what is on `main`.

Daily rather than hourly because Free Edition's fair-use quota shuts compute down for the rest of
the day if exceeded.

Each task ends with an assertion (non-empty output, PR-AUC above a floor). A task that succeeds
while producing garbage is worse than one that fails loudly.

---

## Milestone 8 — Airflow

| Item | Value |
|---|---|
| Airflow | 2.10.3, single container, SQLite + SequentialExecutor |
| Operator | `DatabricksRunNowOperator`, `wait_for_termination=True` |
| Auth | Databricks PAT via `AIRFLOW_CONN_DATABRICKS_DEFAULT`, from gitignored `.env` |

**Databricks Workflows is the scheduler of record**; Airflow is a second control plane triggering
the same job through the Jobs API. A local Airflow only runs while Docker is up, so it cannot be
the thing that guarantees daily execution — but the pattern (Airflow as enterprise orchestrator,
Databricks as execution engine) is how a lot of shops actually run.

`wait_for_termination=True` makes the Airflow task reflect the pipeline's real outcome rather than
just "request submitted."

The SQLite/SequentialExecutor warnings are correct: this is not a production topology. Production
would use Postgres with Celery or Kubernetes workers.

---

## Milestone 9 — Dashboard

| Item | Value |
|---|---|
| Tool | Power BI Desktop |
| Source | `workspace.gold.daily_fraud_summary` (537 rows) via Databricks SQL warehouse |
| Connectivity | **Import**, not DirectQuery |
| Measures | 4 DAX measures |

**Import over DirectQuery** because the SQL warehouse auto-stops when idle (a report would hang
while it wakes) and every interaction would burn Free Edition quota. For a daily-batch pipeline the
data only changes once a day, so a snapshot is the right trade.

**Fraud Rate % is a DAX measure, not the `fraud_rate_pct` column.** Averaging 537 daily percentages
weights a 200-transaction day equally with a 2,400-transaction day. `DIVIDE([Total Fraud],
[Total Transactions])` weights correctly and returns 0.58%, matching the pipeline.

---

## Known limitations

- **Synthetic data.** Model metrics are not representative of real-world fraud detection.
- **Scale.** 1.3M rows justifies Spark but does not stress it. No partition skew or shuffle tuning
  was required.
- **Batch only.** Real fraud detection is substantially real-time; streaming is the obvious extension.
- **Unsalted hash** for card tokenization.
- **Local Airflow** is a demonstration, not a running scheduler.
