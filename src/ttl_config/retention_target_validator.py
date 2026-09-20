from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from ttl_config.retention_rules import RetentionRules

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession


class RetentionTargetValidator:
    """Validate configured retention targets against Unity Catalog."""

    _SUPPORTED_TABLE_FORMATS = {
        "MANAGED": frozenset({"DELTA", "ICEBERG"}),
        "STREAMING_TABLE": frozenset({"DELTA"}),
    }

    _SUPPORTED_TIME_COLUMN_TYPES = frozenset(
        {
            "DATE",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
        }
    )

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def validate_retention_targets(
        self,
        rules: RetentionRules,
    ) -> None:
        """Validate configured tables and their top-level time columns."""
        target_metadata = self._read_target_metadata(rules)

        self._validate_target_tables(target_metadata)
        self._validate_time_columns(target_metadata)

    def _read_target_metadata(
        self,
        rules: RetentionRules,
    ) -> list[Row]:
        configured_targets = self._prepare_configured_targets(rules)
        configured_tables = self._prepare_configured_tables(
            configured_targets
        )

        configured_catalogs = tuple(
            sorted(
                {
                    rule.table_name.catalog
                    for rule in rules
                }
            )
        )
        configured_schemas = tuple(
            sorted(
                {
                    rule.table_name.schema_name
                    for rule in rules
                }
            )
        )

        table_metadata = self._read_table_metadata(
            configured_tables,
            configured_catalogs,
            configured_schemas,
        )
        column_metadata = self._read_column_metadata(
            configured_tables,
            configured_catalogs,
            configured_schemas,
        )

        target_metadata = self._combine_target_metadata(
            configured_targets,
            table_metadata,
            column_metadata,
        )

        return target_metadata.collect()

    def _prepare_configured_targets(
        self,
        rules: RetentionRules,
    ) -> DataFrame:
        rows = [
            (
                rule.table_name.catalog,
                rule.table_name.schema_name,
                rule.table_name.table,
                rule.time_column,
            )
            for rule in rules
        ]

        return self._spark.createDataFrame(
            rows,
            schema=(
                "`catalog` STRING, "
                "`schema` STRING, "
                "`table` STRING, "
                "`time_column` STRING"
            ),
        )

    @staticmethod
    def _prepare_configured_tables(
        configured_targets: DataFrame,
    ) -> DataFrame:
        return configured_targets.select(
            "catalog",
            "schema",
            "table",
        )

    def _read_table_metadata(
        self,
        configured_tables: DataFrame,
        configured_catalogs: tuple[str, ...],
        configured_schemas: tuple[str, ...],
    ) -> DataFrame:
        tables = (
            self._spark.table("system.information_schema.tables")
            .where(
                F.col("table_catalog").isin(*configured_catalogs)
                & F.col("table_schema").isin(*configured_schemas)
            )
            .select(
                F.col("table_catalog").alias("catalog"),
                F.col("table_schema").alias("schema"),
                F.col("table_name").alias("table"),
                F.upper("table_type").alias("table_type"),
                F.upper("data_source_format").alias("data_source_format"),
            )
        )

        return self._filter_to_configured_tables(
            tables,
            configured_tables,
        )

    def _read_column_metadata(
        self,
        configured_tables: DataFrame,
        configured_catalogs: tuple[str, ...],
        configured_schemas: tuple[str, ...],
    ) -> DataFrame:
        columns = (
            self._spark.table("system.information_schema.columns")
            .where(
                F.col("table_catalog").isin(*configured_catalogs)
                & F.col("table_schema").isin(*configured_schemas)
            )
            .select(
                F.col("table_catalog").alias("catalog"),
                F.col("table_schema").alias("schema"),
                F.col("table_name").alias("table"),
                F.col("column_name"),
                F.upper("data_type").alias("data_type"),
            )
        )

        return self._filter_to_configured_tables(
            columns,
            configured_tables,
        )

    @staticmethod
    def _filter_to_configured_tables(
        metadata: DataFrame,
        configured_tables: DataFrame,
    ) -> DataFrame:
        return metadata.join(
            F.broadcast(configured_tables),
            on=["catalog", "schema", "table"],
            how="left_semi",
        )

    def _combine_target_metadata(
        self,
        configured_targets: DataFrame,
        table_metadata: DataFrame,
        column_metadata: DataFrame,
    ) -> DataFrame:
        targets_with_tables = self._join_table_metadata(
            configured_targets,
            table_metadata,
        )

        return self._join_time_column_metadata(
            targets_with_tables,
            column_metadata,
        )

    @staticmethod
    def _join_table_metadata(
        configured_targets: DataFrame,
        table_metadata: DataFrame,
    ) -> DataFrame:
        return configured_targets.join(
            table_metadata,
            on=["catalog", "schema", "table"],
            how="left",
        )

    @staticmethod
    def _join_time_column_metadata(
        target_metadata: DataFrame,
        column_metadata: DataFrame,
    ) -> DataFrame:
        targets = target_metadata.alias("targets")
        columns = column_metadata.alias("columns")

        column_match = (
            (F.col("targets.catalog") == F.col("columns.catalog"))
            & (F.col("targets.schema") == F.col("columns.schema"))
            & (F.col("targets.table") == F.col("columns.table"))
            & (
                F.lower(F.col("targets.time_column"))
                == F.lower(F.col("columns.column_name"))
            )
        )

        return (
            targets
            .join(columns, column_match, "left")
            .select(
                "targets.*",
                F.col("columns.column_name").alias("actual_time_column"),
                F.col("columns.data_type").alias("time_column_type"),
            )
        )

    def _validate_target_tables(
        self,
        target_metadata: list[Row],
    ) -> None:
        self._validate_target_tables_exist(target_metadata)
        self._validate_target_tables_support_retention(target_metadata)

    @staticmethod
    def _validate_target_tables_exist(
        target_metadata: list[Row],
    ) -> None:
        missing_targets = [
            f"`{target['catalog']}`.`{target['schema']}`.`{target['table']}`"
            for target in target_metadata
            if target["table_type"] is None
        ]

        if missing_targets:
            raise ValueError(
                "Retention target tables do not exist or are not visible: "
                + ", ".join(sorted(missing_targets))
            )

    def _validate_target_tables_support_retention(
        self,
        target_metadata: list[Row],
    ) -> None:
        unsupported_targets = [
            (
                f"`{target['catalog']}`.`{target['schema']}`.`{target['table']}` "
                f"({target['table_type']}, {target['data_source_format']})"
            )
            for target in target_metadata
            if not self._table_supports_retention(target)
        ]

        if unsupported_targets:
            raise ValueError(
                "Unsupported retention targets: "
                + ", ".join(sorted(unsupported_targets))
            )

    def _table_supports_retention(
        self,
        target: Row,
    ) -> bool:
        supported_formats = self._SUPPORTED_TABLE_FORMATS.get(
            target["table_type"]
        )

        return (
            supported_formats is not None
            and target["data_source_format"] in supported_formats
        )

    def _validate_time_columns(
        self,
        target_metadata: list[Row],
    ) -> None:
        self._validate_time_columns_exist(target_metadata)
        self._validate_time_column_types(target_metadata)

    @staticmethod
    def _validate_time_columns_exist(
        target_metadata: list[Row],
    ) -> None:
        missing_columns = [
            (
                f"`{target['catalog']}`.`{target['schema']}`.`{target['table']}`"
                f".{target['time_column']}"
            )
            for target in target_metadata
            if target["actual_time_column"] is None
        ]

        if missing_columns:
            raise ValueError(
                "Time columns do not exist: "
                + ", ".join(sorted(missing_columns))
            )

    def _validate_time_column_types(
        self,
        target_metadata: list[Row],
    ) -> None:
        invalid_columns = [
            (
                f"`{target['catalog']}`.`{target['schema']}`.`{target['table']}`"
                f".{target['time_column']} ({target['time_column_type']})"
            )
            for target in target_metadata
            if target["time_column_type"] not in self._SUPPORTED_TIME_COLUMN_TYPES
        ]

        if invalid_columns:
            raise ValueError(
                "Time columns must be DATE, TIMESTAMP, or TIMESTAMP_NTZ: "
                + ", ".join(sorted(invalid_columns))
            )
