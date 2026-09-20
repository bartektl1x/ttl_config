from collections.abc import Iterator
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession

from retention_config.retention_config_generator import RetentionConfigGenerator
from retention_config.retention_rules import RetentionRule, TableName


@pytest.fixture
def retention_destination(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[str, str]]:
    catalog = spark.conf.get("ops_catalog")
    schema = f"retention_config_generator_{uuid4().hex[:8]}"

    monkeypatch.setattr(
        RetentionConfigGenerator,
        "_RETENTION_SCHEMA",
        schema,
    )

    spark.sql(f"CREATE SCHEMA `{catalog}`.`{schema}`")

    try:
        yield catalog, schema
    finally:
        spark.sql(f"DROP SCHEMA `{catalog}`.`{schema}` CASCADE")


def _rule(
    catalog: str,
    schema: str,
    table: str,
    expiration_days: int,
) -> RetentionRule:
    return RetentionRule(
        table_name=TableName(
            catalog=catalog,
            schema=schema,
            table=table,
        ),
        time_column="event_time",
        expiration_days=expiration_days,
    )


def test_write_retention_config_creates_managed_unpartitioned_delta_table(
    spark: SparkSession,
    retention_destination: tuple[str, str],
) -> None:
    # given
    catalog, schema = retention_destination
    generator = RetentionConfigGenerator(spark)
    rules = (
        _rule(catalog, schema, "orders", 30),
        _rule(catalog, schema, "customers", 90),
    )
    identifier = f"`{catalog}`.`{schema}`.`retention_config`"

    # when
    generator.write_retention_config(rules)

    # then
    table = spark.catalog.getTable(identifier)
    detail = spark.sql(f"DESCRIBE DETAIL {identifier}").first()
    rows = {
        tuple(row)
        for row in spark.table(identifier).collect()
    }

    assert table.tableType == "MANAGED"
    assert detail["format"].lower() == "delta"
    assert detail["partitionColumns"] == []
    assert spark.table(identifier).schema.simpleString() == (
        "struct<catalog:string,schema:string,table:string,"
        "time_column:string,expiration_days:bigint>"
    )
    assert rows == {
        (catalog, schema, "orders", "event_time", 30),
        (catalog, schema, "customers", "event_time", 90),
    }


def test_write_retention_config_replaces_authoritative_snapshot(
    spark: SparkSession,
    retention_destination: tuple[str, str],
) -> None:
    # given
    catalog, schema = retention_destination
    generator = RetentionConfigGenerator(spark)
    initial_rules = (
        _rule(catalog, schema, "orders", 30),
        _rule(catalog, schema, "customers", 90),
    )
    replacement_rules = (
        _rule(catalog, schema, "orders", 60),
        _rule(catalog, schema, "payments", 120),
    )
    identifier = f"`{catalog}`.`{schema}`.`retention_config`"

    generator.write_retention_config(initial_rules)

    # when
    generator.write_retention_config(replacement_rules)

    # then
    rows = {
        tuple(row)
        for row in spark.table(identifier).collect()
    }
    assert rows == {
        (catalog, schema, "orders", "event_time", 60),
        (catalog, schema, "payments", "event_time", 120),
    }


def test_write_retention_config_rejects_incompatible_existing_table_without_modifying_it(
    spark: SparkSession,
    retention_destination: tuple[str, str],
) -> None:
    # given
    catalog, schema = retention_destination
    identifier = f"`{catalog}`.`{schema}`.`retention_config`"

    spark.sql(
        f"""
        CREATE TABLE {identifier} (
            `catalog` STRING,
            `schema` STRING,
            `table` STRING,
            `time_column` STRING,
            `expiration_days` BIGINT
        )
        USING DELTA
        PARTITIONED BY (`catalog`)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {identifier}
        VALUES ('sentinel_catalog', 'sentinel_schema', 'sentinel_table', 'event_time', 7)
        """
    )

    generator = RetentionConfigGenerator(spark)
    replacement_rules = (
        _rule(catalog, schema, "orders", 30),
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="must be an unpartitioned Delta table",
    ):
        generator.write_retention_config(replacement_rules)

    rows = {
        tuple(row)
        for row in spark.table(identifier).collect()
    }
    assert rows == {
        (
            "sentinel_catalog",
            "sentinel_schema",
            "sentinel_table",
            "event_time",
            7,
        )
    }
