from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from retention_config.retention_config_generator import RetentionConfigGenerator
from retention_config.retention_rules import RetentionRule, TableName


def _table(name: str) -> TableName:
    return TableName(
        catalog="main",
        schema="retention",
        table=name,
    )


def _rule(
    table: str,
    expiration_days: int = 30,
) -> RetentionRule:
    return RetentionRule(
        table_name=_table(table),
        time_column="event_time",
        expiration_days=expiration_days,
    )


def _output_schema() -> StructType:
    return StructType(
        [
            StructField("catalog", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("time_column", StringType(), True),
            StructField("expiration_days", LongType(), True),
        ]
    )


def _generator() -> tuple[RetentionConfigGenerator, MagicMock]:
    spark = MagicMock()
    spark.conf.get.return_value = "ops"
    return RetentionConfigGenerator(spark), spark


def _prepared_config(schema: StructType | None = None) -> MagicMock:
    retention_config = MagicMock()
    retention_config.schema = schema or _output_schema()
    retention_config.write.option.return_value = retention_config.write
    return retention_config


def test_generate_retention_config_without_inheritance_validates_explicit_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    generator, _ = _generator()
    explicit_rules = (_rule("orders"),)

    read_rules = MagicMock(return_value=explicit_rules)
    inherit_rules = MagicMock(
        side_effect=AssertionError("Inheritance must not be used"),
    )
    validate_targets = MagicMock()

    monkeypatch.setattr(generator, "_read_retention_rules", read_rules)
    monkeypatch.setattr(generator, "_inherit_retention_rules", inherit_rules)
    monkeypatch.setattr(generator, "_validate_retention_targets", validate_targets)

    # when
    rules = generator.generate_retention_config(
        "retention.xlsx",
        "Retention",
    )

    # then
    assert rules == explicit_rules
    read_rules.assert_called_once_with(
        "retention.xlsx",
        "Retention",
    )
    inherit_rules.assert_not_called()
    validate_targets.assert_called_once_with(explicit_rules)


def test_generate_retention_config_with_inheritance_validates_effective_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    generator, _ = _generator()
    explicit_rules = (_rule("orders"),)
    effective_rules = (
        *explicit_rules,
        _rule("customers"),
    )

    read_rules = MagicMock(return_value=explicit_rules)
    inherit_rules = MagicMock(return_value=effective_rules)
    validate_targets = MagicMock()

    monkeypatch.setattr(generator, "_read_retention_rules", read_rules)
    monkeypatch.setattr(generator, "_inherit_retention_rules", inherit_rules)
    monkeypatch.setattr(generator, "_validate_retention_targets", validate_targets)

    # when
    rules = generator.generate_retention_config(
        "retention.xlsx",
        "Retention",
        include_inheritance=True,
    )

    # then
    assert rules == effective_rules
    inherit_rules.assert_called_once_with(explicit_rules)
    validate_targets.assert_called_once_with(effective_rules)


def test_write_retention_config_uses_errorifexists_for_new_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    generator, spark = _generator()
    rules = (_rule("orders"),)
    retention_config = _prepared_config()
    spark.catalog.tableExists.return_value = False

    validate_destination = MagicMock()
    monkeypatch.setattr(
        generator,
        "_prepare_retention_config",
        lambda _: retention_config,
    )
    monkeypatch.setattr(
        generator,
        "_validate_retention_config_table",
        validate_destination,
    )

    # when
    generator.write_retention_config(rules)

    # then
    identifier = "`ops`.`retention`.`retention_config`"
    spark.catalog.tableExists.assert_called_once_with(identifier)
    validate_destination.assert_not_called()
    retention_config.write.option.assert_called_once_with(
        "mergeSchema",
        "false",
    )
    retention_config.write.saveAsTable.assert_called_once_with(
        identifier,
        format="delta",
        mode="errorifexists",
    )


def test_write_retention_config_rejects_non_managed_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    generator, spark = _generator()
    rules = (_rule("orders"),)
    retention_config = _prepared_config()

    spark.catalog.tableExists.return_value = True
    spark.catalog.getTable.return_value = MagicMock(
        tableType="EXTERNAL",
    )
    monkeypatch.setattr(
        generator,
        "_prepare_retention_config",
        lambda _: retention_config,
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="must be a managed Delta table",
    ):
        generator.write_retention_config(rules)

    retention_config.write.saveAsTable.assert_not_called()


@pytest.mark.parametrize(
    ("format_name", "partition_columns"),
    [
        ("parquet", []),
        ("delta", ["catalog"]),
    ],
)
def test_write_retention_config_rejects_invalid_destination_format(
    monkeypatch: pytest.MonkeyPatch,
    format_name: str,
    partition_columns: list[str],
) -> None:
    # given
    generator, spark = _generator()
    rules = (_rule("orders"),)
    retention_config = _prepared_config()

    spark.catalog.tableExists.return_value = True
    spark.catalog.getTable.return_value = MagicMock(
        tableType="MANAGED",
    )
    spark.sql.return_value.first.return_value = Row(
        format=format_name,
        partitionColumns=partition_columns,
    )
    monkeypatch.setattr(
        generator,
        "_prepare_retention_config",
        lambda _: retention_config,
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="must be an unpartitioned Delta table",
    ):
        generator.write_retention_config(rules)

    retention_config.write.saveAsTable.assert_not_called()


def test_write_retention_config_rejects_unexpected_destination_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    generator, spark = _generator()
    rules = (_rule("orders"),)
    expected_schema = _output_schema()
    retention_config = _prepared_config(expected_schema)

    spark.catalog.tableExists.return_value = True
    spark.catalog.getTable.return_value = MagicMock(
        tableType="MANAGED",
    )
    spark.sql.return_value.first.return_value = Row(
        format="delta",
        partitionColumns=[],
    )
    spark.table.return_value.schema = StructType(
        [
            StructField("catalog", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("time_column", StringType(), True),
            StructField("expiration_days", StringType(), True),
        ]
    )
    monkeypatch.setattr(
        generator,
        "_prepare_retention_config",
        lambda _: retention_config,
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="has an unexpected schema",
    ):
        generator.write_retention_config(rules)

    retention_config.write.saveAsTable.assert_not_called()
