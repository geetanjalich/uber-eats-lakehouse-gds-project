import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create one lightweight local Spark session shared by all transformation tests."""

    # Point PySpark at the same Python executable running pytest. This avoids common
    # laptop issues where the driver and worker processes resolve different interpreters.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Two local threads are enough to exercise Spark execution plans while keeping test
    # startup cost low. Shuffle partitions are reduced because fixtures are tiny.
    session = (
        SparkSession.builder.master("local[2]")
        .appName("uber-eats-lakehouse-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
