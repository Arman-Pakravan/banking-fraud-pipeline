# Databricks notebook source
# MAGIC %md
# MAGIC # Silver -> Gold
# MAGIC
# MAGIC Builds two gold tables from the clean silver table:
# MAGIC
# MAGIC - `transaction_features` -- one row per transaction, with per-card behavioural
# MAGIC   features computed over time windows, for the model to train on.
# MAGIC - `daily_fraud_summary` -- one row per day, for the dashboard.
# MAGIC
# MAGIC All feature logic lives in `src/transforms.py` and is covered by unit tests,
# MAGIC including a regression test that the 24-hour window is time-based rather than
# MAGIC row-based.

# COMMAND ----------

import sys, os

sys.path.append(os.path.abspath(".."))

from src.transforms import (
    add_card_features, add_merchant_distance, add_temporal_features,
    build_daily_summary,
)

# COMMAND ----------

import time

CATALOG       = "workspace"
SILVER_TABLE  = f"{CATALOG}.silver.transactions_clean"
FEATURE_TABLE = f"{CATALOG}.gold.transaction_features"
DAILY_TABLE   = f"{CATALOG}.gold.daily_fraud_summary"

# COMMAND ----------

start  = time.time()
silver = spark.table(SILVER_TABLE)

features = add_temporal_features(
    add_merchant_distance(
        add_card_features(silver)))

(features.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(FEATURE_TABLE))

# COMMAND ----------

daily = build_daily_summary(features)

(daily.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(DAILY_TABLE))

# COMMAND ----------

feature_rows = features.count()
daily_rows   = daily.count()
elapsed      = time.time() - start

print(f"feature rows:  {feature_rows:,}")
print(f"feature cols:  {len(features.columns)}")
print(f"daily rows:    {daily_rows:,}")
print(f"elapsed:       {elapsed:.1f}s")

assert feature_rows > 0, "Gold feature table is empty -- upstream silver may be missing."
