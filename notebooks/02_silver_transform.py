# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver
# MAGIC
# MAGIC Reads the raw bronze table, applies the tested transforms from `src/transforms.py`,
# MAGIC and writes two tables: the clean silver table and a quarantine table holding every
# MAGIC row that failed a data-quality rule, with the reason attached.
# MAGIC
# MAGIC This notebook contains no transform logic of its own. Everything it calls is
# MAGIC covered by the unit tests that gate pull requests in CI.

# COMMAND ----------

import sys, os

# The Git folder puts the repo root one level above this notebook.
sys.path.append(os.path.abspath(".."))

from src.transforms import (
    cast_types, flag_quality, split_quarantine, mask_pii, deduplicate,
)

# COMMAND ----------

import time
from pyspark.sql import functions as F

CATALOG          = "workspace"
BRONZE_TABLE     = f"{CATALOG}.bronze.transactions_raw"
SILVER_TABLE     = f"{CATALOG}.silver.transactions_clean"
QUARANTINE_TABLE = f"{CATALOG}.silver.transactions_quarantine"

# COMMAND ----------

start   = time.time()
bronze  = spark.table(BRONZE_TABLE)
rows_in = bronze.count()

flagged = flag_quality(cast_types(bronze)).cache()
passed, quarantined = split_quarantine(flagged)

masked             = mask_pii(passed)
rows_before_dedupe = masked.count()
deduped            = deduplicate(masked)
rows_out           = deduped.count()
dupes_removed      = rows_before_dedupe - rows_out

# COMMAND ----------

(deduped.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(SILVER_TABLE))

(quarantined.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(QUARANTINE_TABLE))

# COMMAND ----------

q_count    = quarantined.count()
fraud_rows = deduped.filter(F.col("is_fraud") == 1).count()
elapsed    = time.time() - start

print(f"rows in:            {rows_in:,}")
print(f"quarantined:        {q_count:,}  ({q_count/rows_in:.4%})")
print(f"duplicates removed: {dupes_removed:,}")
print(f"rows out:           {rows_out:,}")
print(f"fraud rows:         {fraud_rows:,}")
print(f"fraud rate:         {fraud_rows/rows_out:.4%}")
print(f"elapsed:            {elapsed:.1f}s")

# A job task that silently writes zero rows is worse than one that fails loudly.
assert rows_out > 0, "Silver table is empty -- upstream bronze may be missing."
