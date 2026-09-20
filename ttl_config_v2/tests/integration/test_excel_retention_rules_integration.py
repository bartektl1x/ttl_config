from pathlib import Path

from pyspark.sql import SparkSession

from retention_config.excel_retention_rules import ExcelRetentionRulesReader
from retention_config.retention_rules import RetentionRule, TableName


def test_read_retention_rules_from_real_workbook(
    spark: SparkSession,
) -> None:
    # given
    workbook_path = (
        Path(__file__).parents[1]
        / "resources"
        / "retention_rules.xlsx"
    )
    reader = ExcelRetentionRulesReader(spark)

    # when
    rules = reader.read_retention_rules(
        workbook_path.as_uri(),
        "Retention",
    )

    # then
    assert rules == (
        RetentionRule(
            table_name=TableName(
                catalog="main",
                schema="sales",
                table="orders",
            ),
            time_column="EventTime",
            expiration_days=30,
        ),
        RetentionRule(
            table_name=TableName(
                catalog="main",
                schema="sales",
                table="customers",
            ),
            time_column="created_at",
            expiration_days=90,
        ),
    )
