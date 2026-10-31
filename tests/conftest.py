"""Shared pytest fixtures.

The `spark` fixture is session-scoped because starting a SparkSession takes
several seconds; starting one per test would make the suite unusably slow.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local[2]")
               .appName("fraud-pipeline-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
