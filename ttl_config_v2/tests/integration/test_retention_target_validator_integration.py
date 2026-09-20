from collections.abc import Iterator
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession

from retention_config.retention_rules import RetentionRule, TableName
from retention_config.retention_target_validator import RetentionTargetValidator


@pytest.fixture
def target_schema(
    spark: SparkSession,
) -> Iterator[tuple[str, str]]:
    catalog = spark.conf.get("ops_catalog")
    schema = f"retention_target_validator_{uuid4().hex[:8]}"

    spark.sql(f"CREATE SCHEMA `{catalog}`.`{schema}`")

    try:
        yield catalog, schema
    finally:
        spark.sql(f"DROP SCHEMA `{catalog}`.`{schema}` CASCADE")


def test_validate_retention_targets_with_real_managed_delta_table(
    spark: SparkSession,
    target_schema: tuple[str, str],
) -> None:
    # given
    catalog, schema = target_schema
    spark.sql(
        f"""
        CREATE TABLE `{catalog}`.`{schema}`.`orders` (
            order_id BIGINT,
            event_time TIMESTAMP
        )
        USING DELTA
        """
    )
    rule = RetentionRule(
        table_name=TableName(
            catalog=catalog,
            schema=schema,
            table="orders",
        ),
        time_column="EVENT_TIME",
        expiration_days=30,
    )
    validator = RetentionTargetValidator(spark)

    # when / then
    validator.validate_retention_targets((rule,))


def test_validate_retention_targets_rejects_real_view(
    spark: SparkSession,
    target_schema: tuple[str, str],
) -> None:
    # given
    catalog, schema = target_schema
    spark.sql(
        f"""
        CREATE VIEW `{catalog}`.`{schema}`.`orders_view`
        AS SELECT current_timestamp() AS event_time
        """
    )
    rule = RetentionRule(
        table_name=TableName(
            catalog=catalog,
            schema=schema,
            table="orders_view",
        ),
        time_column="event_time",
        expiration_days=30,
    )
    validator = RetentionTargetValidator(spark)

    # when / then
    with pytest.raises(
        ValueError,
        match="Unsupported retention targets",
    ):
        validator.validate_retention_targets((rule,))
