import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("inheritance-v2-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.udf.register(
        "current_metastore", lambda: "cloud:region:metastore-1", StringType()
    )
    yield session
    session.stop()
