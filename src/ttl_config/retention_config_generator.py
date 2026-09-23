from __future__ import annotations

from typing import TYPE_CHECKING

from ttl_config.excel_retention_rules import ExcelRetentionRulesReader
from ttl_config.retention_rules import (
    RetentionRules,
    TableName,
)
from ttl_config.retention_target_validator import RetentionTargetValidator

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


class RetentionConfigGenerator:
    """Generate validated rules and explicitly write an authoritative snapshot."""

    _OPS_CATALOG_CONFIG = "ops_catalog"
    _RETENTION_SCHEMA = "retention"
    _RETENTION_TABLE = "ttl_config"

    _OUTPUT_SCHEMA = (
        "`catalog` STRING, "
        "`schema` STRING, "
        "`table` STRING, "
        "`time_column` STRING, "
        "`expiration_days` BIGINT"
    )

    def __init__(
        self,
        spark: SparkSession,
    ) -> None:
        self._spark = spark

        self._rules_reader = ExcelRetentionRulesReader(spark)
        self._target_validator = RetentionTargetValidator(spark)

    def generate_ttl_config(
        self,
        workbook_path: str,
        sheet_name: str,
        *,
        include_inheritance: bool = False,
    ) -> RetentionRules:
        """Read and validate rules without writing configuration or target tables.

        With include_inheritance=False, the resolver is neither imported,
        constructed, nor called. Destination configuration is needed only when
        write_ttl_config is called.
        """
        rules = self._read_retention_rules(
            workbook_path,
            sheet_name,
        )

        if include_inheritance:
            rules = self._inherit_retention_rules(rules)

        self._validate_retention_targets(rules)

        return rules

    def write_ttl_config(
        self,
        rules: RetentionRules,
    ) -> None:
        """Replace the destination with the complete generated rules snapshot.

        Pass the rules returned by generate_ttl_config. Omitted targets
        are intentionally removed from configuration. Removing active target
        policies is the downstream TTL applicator's responsibility.
        """
        table_name = self._resolve_ttl_config_table()
        ttl_config = self._prepare_ttl_config(rules)
        destination_exists = self._spark.catalog.tableExists(
            table_name.sql_identifier
        )

        if destination_exists:
            self._validate_ttl_config_table(
                table_name,
                ttl_config.schema,
            )

        # Do not overwrite an unchecked table created after the existence check.
        mode = "overwrite" if destination_exists else "errorifexists"

        ttl_config.write.option(
            "mergeSchema",
            "false",
        ).saveAsTable(
            table_name.sql_identifier,
            format="delta",
            mode=mode,
        )

    def _read_retention_rules(
        self,
        workbook_path: str,
        sheet_name: str,
    ) -> RetentionRules:
        return self._rules_reader.read_retention_rules(
            workbook_path,
            sheet_name,
        )

    def _inherit_retention_rules(
        self,
        rules: RetentionRules,
    ) -> RetentionRules:
        from ttl_config.retention_rule_inheritance import (
            RetentionRuleInheritanceResolver,
        )

        resolver = RetentionRuleInheritanceResolver(self._spark)

        return resolver.inherit_retention_rules(rules)

    def _validate_retention_targets(
        self,
        rules: RetentionRules,
    ) -> None:
        self._target_validator.validate_retention_targets(
            rules
        )

    def _resolve_ttl_config_table(self) -> TableName:
        return TableName(
            catalog=self._spark.conf.get(self._OPS_CATALOG_CONFIG),
            schema=self._RETENTION_SCHEMA,
            table=self._RETENTION_TABLE,
        )

    def _validate_ttl_config_table(
        self,
        table_name: TableName,
        expected_schema: StructType,
    ) -> None:
        self._validate_ttl_config_table_type(table_name)
        self._validate_ttl_config_table_format(table_name)
        self._validate_ttl_config_table_schema(
            table_name,
            expected_schema,
        )

    def _validate_ttl_config_table_type(
        self,
        table_name: TableName,
    ) -> None:
        identifier = table_name.sql_identifier
        table = self._spark.catalog.getTable(identifier)

        if table.tableType != "MANAGED":
            raise ValueError(
                f"Retention configuration destination {identifier} must be a "
                f"managed Delta table; found {table.tableType}"
            )

    def _validate_ttl_config_table_format(
        self,
        table_name: TableName,
    ) -> None:
        identifier = table_name.sql_identifier
        detail = self._spark.sql(
            f"DESCRIBE DETAIL {identifier}"
        ).first()

        # A partitioned destination can retain omitted rules under dynamic
        # partition overwrite; this table is always a complete snapshot.
        if detail["format"].lower() != "delta" or detail["partitionColumns"]:
            raise ValueError(
                f"Retention configuration destination {identifier} must be an "
                f"unpartitioned Delta table; found format={detail['format']!r}, "
                f"partitionColumns={detail['partitionColumns']!r}"
            )

    def _validate_ttl_config_table_schema(
        self,
        table_name: TableName,
        expected_schema: StructType,
    ) -> None:
        identifier = table_name.sql_identifier
        actual_schema = self._spark.table(identifier).schema

        actual_columns = [
            (field.name, field.dataType)
            for field in actual_schema
        ]
        expected_columns = [
            (field.name, field.dataType)
            for field in expected_schema
        ]

        if actual_columns != expected_columns:
            raise ValueError(
                f"Retention configuration destination {identifier} has an "
                f"unexpected schema: expected {expected_schema.simpleString()}, "
                f"found {actual_schema.simpleString()}"
            )

    def _prepare_ttl_config(
        self,
        rules: RetentionRules,
    ) -> DataFrame:
        rows = [
            (
                rule.table_name.catalog,
                rule.table_name.schema_name,
                rule.table_name.table,
                rule.time_column,
                rule.expiration_days,
            )
            for rule in rules
        ]

        return self._spark.createDataFrame(
            rows,
            schema=self._OUTPUT_SCHEMA,
        )
