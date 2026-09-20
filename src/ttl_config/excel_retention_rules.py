from __future__ import annotations

from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

from ttl_config.retention_rules import (
    RetentionRule,
    RetentionRules,
    TableName,
)

if TYPE_CHECKING:
    from pyspark.sql import Row, SparkSession
    from pyspark.sql.readwriter import DataFrameReader


class ExcelRetentionRulesReader:
    """Read retention rules from a product-owned Excel worksheet."""

    _REQUIRED_COLUMNS = frozenset(
        {
            "catalog",
            "schema",
            "table",
            "time_column",
            "expiration_days",
        }
    )

    def __init__(
        self,
        spark: SparkSession,
        *,
        max_rows: int = 10_000,
    ) -> None:
        if type(max_rows) is not int or max_rows < 1:
            raise ValueError("max_rows must be a positive integer")

        self._spark = spark
        self._max_rows = max_rows

    def read_retention_rules(
        self,
        workbook_path: str,
        sheet_name: str,
    ) -> RetentionRules:
        """Read and validate retention rules from one Excel worksheet."""
        self._validate_workbook_input(
            workbook_path,
            sheet_name,
        )

        reader = self._create_excel_reader(sheet_name)

        headers = self._read_headers(
            reader,
            workbook_path,
        )

        columns = self._normalize_headers(headers)
        self._validate_headers(columns)

        rows = self._read_rows(
            reader,
            workbook_path,
            headers,
            sheet_name,
        )

        rules = self._parse_retention_rules(
            columns,
            rows,
            sheet_name,
        )

        self._validate_workbook_rules(
            rules,
            sheet_name,
        )

        return rules

    @staticmethod
    def _validate_workbook_input(
        workbook_path: str,
        sheet_name: str,
    ) -> None:
        if not workbook_path.strip():
            raise ValueError("Workbook path must not be empty")

        if not sheet_name.strip():
            raise ValueError("Worksheet name must not be empty")

    def _create_excel_reader(
        self,
        sheet_name: str,
    ) -> DataFrameReader:
        return (
            self._spark.read
            .format("excel")
            .option("headerRows", 1)
            .option("dataAddress", sheet_name)
        )

    @staticmethod
    def _read_headers(
        reader: DataFrameReader,
        workbook_path: str,
    ) -> list[str]:
        return reader.load(workbook_path).columns

    @staticmethod
    def _normalize_headers(
        headers: list[str],
    ) -> tuple[str, ...]:
        return tuple(
            header.strip().lower()
            for header in headers
        )

    def _validate_headers(
        self,
        columns: tuple[str, ...],
    ) -> None:
        counts = Counter(columns)

        missing = sorted(self._REQUIRED_COLUMNS - counts.keys())
        unexpected = sorted(counts.keys() - self._REQUIRED_COLUMNS)
        duplicated = sorted(
            column
            for column, count in counts.items()
            if count > 1
        )

        if missing or unexpected or duplicated:
            raise ValueError(
                "Expected exactly catalog, schema, table, time_column, expiration_days; "
                f"missing={missing}, unexpected={unexpected}, duplicated={duplicated}"
            )

    def _read_rows(
        self,
        reader: DataFrameReader,
        workbook_path: str,
        headers: list[str],
        sheet_name: str,
    ) -> list[Row]:
        schema = ", ".join(
            f"`{header.replace('`', '``')}` STRING"
            for header in headers
        )

        rows = (
            reader.schema(schema)
            .load(workbook_path)
            .limit(self._max_rows + 1)
            .collect()
        )

        if len(rows) > self._max_rows:
            raise ValueError(
                f"Worksheet {sheet_name!r} exceeds max_rows={self._max_rows}"
            )

        return rows

    def _parse_retention_rules(
        self,
        columns: tuple[str, ...],
        rows: list[Row],
        sheet_name: str,
    ) -> RetentionRules:
        rules: list[RetentionRule] = []

        for row in rows:
            if all(
                value is None or str(value).strip() == ""
                for value in row
            ):
                continue

            rule = self._parse_retention_rule(
                columns,
                row,
                sheet_name,
            )
            rules.append(rule)

        return tuple(rules)

    def _parse_retention_rule(
        self,
        columns: tuple[str, ...],
        row: Row,
        sheet_name: str,
    ) -> RetentionRule:
        fields = dict(zip(columns, row, strict=True))

        try:
            return RetentionRule(
                table_name=TableName(
                    catalog=fields["catalog"],
                    schema=fields["schema"],
                    table=fields["table"],
                ),
                time_column=fields["time_column"],
                expiration_days=self._parse_expiration_days(
                    fields["expiration_days"]
                ),
            )
        except ValueError as error:
            target = tuple(
                fields.get(column)
                for column in ("catalog", "schema", "table")
            )

            raise ValueError(
                f"Worksheet {sheet_name!r}, target {target}: {error}"
            ) from error

    @staticmethod
    def _parse_expiration_days(
        value: str | None,
    ) -> int:
        if value is None:
            raise ValueError(
                "expiration_days must be a whole number"
            )

        try:
            days = Decimal(value.strip())
        except InvalidOperation:
            raise ValueError(
                "expiration_days must be a whole number"
            ) from None

        if not days.is_finite() or days != days.to_integral_value():
            raise ValueError(
                "expiration_days must be a whole number"
            )

        return int(days)

    def _validate_workbook_rules(
        self,
        rules: RetentionRules,
        sheet_name: str,
    ) -> None:
        self._validate_rules_not_empty(
            rules,
            sheet_name,
        )
        self._validate_unique_targets(
            rules,
            sheet_name,
        )

    @staticmethod
    def _validate_rules_not_empty(
        rules: RetentionRules,
        sheet_name: str,
    ) -> None:
        if not rules:
            raise ValueError(
                f"Worksheet {sheet_name!r} contains no retention rules"
            )

    @staticmethod
    def _validate_unique_targets(
        rules: RetentionRules,
        sheet_name: str,
    ) -> None:
        seen: set[TableName] = set()

        for rule in rules:
            if rule.table_name in seen:
                raise ValueError(
                    f"Worksheet {sheet_name!r} contains duplicate retention rule for "
                    f"{rule.table_name.sql_identifier}"
                )

            seen.add(rule.table_name)
