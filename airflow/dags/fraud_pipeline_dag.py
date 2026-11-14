"""Trigger the Databricks fraud pipeline from Airflow.

Databricks Workflows is the scheduler of record for this project -- it runs
daily in the cloud whether or not a laptop is switched on. This DAG is a second
entry point to the same job, via the Databricks Jobs API.

That split mirrors how a lot of shops actually run: Airflow as the enterprise
control plane owning cross-system dependencies, Databricks as the execution
engine. The DAG has one task today, but the shape extends -- wait for an
upstream feed, trigger Databricks, kick off a downstream report.

The schedule is None on purpose. A local Airflow only runs while Docker is up,
so an automatic schedule would log misleading missed runs. Trigger it manually.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)

DATABRICKS_JOB_ID = os.environ["DATABRICKS_JOB_ID"]

default_args = {
    "owner": "arman",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_pipeline_trigger",
    description="Trigger the medallion fraud pipeline running on Databricks",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger -- see module docstring
    catchup=False,
    default_args=default_args,
    tags=["fraud", "databricks", "medallion"],
) as dag:

    run_pipeline = DatabricksRunNowOperator(
        task_id="run_fraud_pipeline",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        # Block until the Databricks job finishes, so this task's state
        # reflects the pipeline's real outcome rather than just "submitted".
        wait_for_termination=True,
        polling_period_seconds=30,
    )
