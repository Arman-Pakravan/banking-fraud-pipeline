# Databricks notebook source
# MAGIC %md
# MAGIC # Raw CSV -> Bronze
# MAGIC
# MAGIC Lands the source CSV in a Delta table with ingestion metadata attached.
# MAGIC
# MAGIC Every column lands as STRING on purpose. Schema inference costs an extra full
# MAGIC scan of the file and is non-deterministic across batches -- a column that is all
# MAGIC digits today infers as integer, and one bad value tomorrow infers as string,
# MAGIC breaking everything downstream. Casting happens in silver, where it is explicit,
# MAGIC reviewable and covered by tests.
# MAGIC
# MAGIC The CSV is uploaded by hand to a Unity Catalog volume because Databricks Free
# MAGIC Edition restricts outbound internet from compute to a trusted-domain allowlist.
# MAGIC In production this would be a cloud landing zone read incrementally with Auto Loader.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_files;

# COMMAND ----------

import uuid, time
from pyspark.sql import functions as F

CATALOG      = "workspace"
RAW_FILE     = "fraudTrain.csv"
RAW_PATH     = f"/Volumes/{CATALOG}/bronze/raw_files/{RAW_FILE}"
BRONZE_TABLE = f"{CATALOG}.bronze.transactions_raw"

# COMMAND ----------

start    = time.time()
batch_id = str(uuid.uuid4())

raw = (spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "false")
         .load(RAW_PATH))

bronze = (raw
          .withColumn("_ingest_ts",   F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_batch_id",    F.lit(batch_id)))

(bronze.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(BRONZE_TABLE))

# COMMAND ----------

rows    = bronze.count()
elapsed = time.time() - start

print(f"rows:     {rows:,}")
print(f"columns:  {len(bronze.columns)}")
print(f"batch_id: {batch_id}")
print(f"elapsed:  {elapsed:.1f}s")

assert rows > 0, f"Bronze ingest produced zero rows -- is {RAW_FILE} present in the volume?"