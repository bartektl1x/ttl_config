from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row

from retention_config.excel_retention_rules import ExcelRetentionRulesReader
from retention_config.retention_rules import RetentionRule, TableName


_REQUIRED_HEADERS = [
    "catalog",
    "schema",
    "table",
    "time_column",
    "expiration_days",
]


def _reader_with(
    monkeypatch: pytest.MonkeyPatch,
    *,
    headers: list[str],
    rows: list[Row],
    max_rows: int = 10_000,
) -> ExcelRetentionRulesReader:
    excel_reader = MagicMock()

    header_frame = MagicMock()
    header_frame.columns = headers

    data_frame = MagicMock()
    data_frame.limit.return_value.collect.return_value = rows

    excel_reader.load.side_effect = [header_frame, data_frame]
    excel_reader.schema.return_value = excel_reader

    reader = ExcelRetentionRulesReader(
        MagicMock(),
        max_rows=max_rows,
    )
    monkeypatch.setattr(
        reader,
        "_create_excel_reader",
        lambda _: excel_reader,
    )
    return reader


def test_read_retention_rules_parses_valid_rows_and_ignores_blank_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    headers = [
        " TABLE ",
        "Expiration_Days",
        " Catalog ",
        "TIME_COLUMN ",
        " Schema ",
    ]
    rows = [
        Row("Orders", "30.0", "MAIN", "EventTime", "Sales"),
        Row(None, None, None, None, None),
        Row("Customers", "90", "main", "created_at", "sales"),
    ]
    reader = _reader_with(
        monkeypatch,
        headers=headers,
        rows=rows,
    )

    # when
    rules = reader.read_retention_rules(
        "retention.xlsx",
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


@pytest.mark.parametrize(
    ("workbook_path", "sheet_name", "message"),
    [
        ("   ", "Retention", "Workbook path must not be empty"),
        ("retention.xlsx", "   ", "Worksheet name must not be empty"),
    ],
)
def test_read_retention_rules_rejects_invalid_workbook_input(
    workbook_path: str,
    sheet_name: str,
    message: str,
) -> None:
    # given
    reader = ExcelRetentionRulesReader(MagicMock())

    # when / then
    with pytest.raises(ValueError, match=message):
        reader.read_retention_rules(workbook_path, sheet_name)


@pytest.mark.parametrize(
    ("headers", "message"),
    [
        (
            ["catalog", "schema", "table", "time_column"],
            r"missing=\['expiration_days'\]",
        ),
        (
            [*_REQUIRED_HEADERS, "owner"],
            r"unexpected=\['owner'\]",
        ),
        (
            [*_REQUIRED_HEADERS, " Catalog "],
            r"duplicated=\['catalog'\]",
        ),
    ],
)
def test_read_retention_rules_rejects_invalid_headers(
    monkeypatch: pytest.MonkeyPatch,
    headers: list[str],
    message: str,
) -> None:
    # given
    reader = _reader_with(
        monkeypatch,
        headers=headers,
        rows=[],
    )

    # when / then
    with pytest.raises(ValueError, match=message):
        reader.read_retention_rules(
            "retention.xlsx",
            "Retention",
        )


def test_read_retention_rules_rejects_empty_worksheet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    rows = [
        Row(None, None, None, None, None),
        Row(" ", "", None, "   ", None),
    ]
    reader = _reader_with(
        monkeypatch,
        headers=_REQUIRED_HEADERS,
        rows=rows,
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="Worksheet 'Retention' contains no retention rules",
    ):
        reader.read_retention_rules(
            "retention.xlsx",
            "Retention",
        )


def test_read_retention_rules_rejects_duplicate_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    rows = [
        Row("main", "sales", "orders", "created_at", "30"),
        Row("main", "sales", "orders", "updated_at", "90"),
    ]
    reader = _reader_with(
        monkeypatch,
        headers=_REQUIRED_HEADERS,
        rows=rows,
    )

    # when / then
    with pytest.raises(
        ValueError,
        match=r"duplicate retention rule for `main`\.`sales`\.`orders`",
    ):
        reader.read_retention_rules(
            "retention.xlsx",
            "Retention",
        )


@pytest.mark.parametrize(
    "expiration_days",
    [
        None,
        "not-a-number",
        "30.5",
        "NaN",
    ],
)
def test_read_retention_rules_rejects_invalid_expiration_days(
    monkeypatch: pytest.MonkeyPatch,
    expiration_days: str | None,
) -> None:
    # given
    rows = [
        Row(
            "main",
            "sales",
            "orders",
            "created_at",
            expiration_days,
        ),
    ]
    reader = _reader_with(
        monkeypatch,
        headers=_REQUIRED_HEADERS,
        rows=rows,
    )

    # when / then
    with pytest.raises(ValueError) as error:
        reader.read_retention_rules(
            "retention.xlsx",
            "Retention",
        )

    assert "Worksheet 'Retention'" in str(error.value)
    assert "('main', 'sales', 'orders')" in str(error.value)
    assert "expiration_days must be a whole number" in str(error.value)


def test_read_retention_rules_rejects_row_limit_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    rows = [
        Row("main", "sales", "orders", "created_at", "30"),
        Row("main", "sales", "customers", "created_at", "30"),
    ]
    reader = _reader_with(
        monkeypatch,
        headers=_REQUIRED_HEADERS,
        rows=rows,
        max_rows=1,
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="Worksheet 'Retention' exceeds max_rows=1",
    ):
        reader.read_retention_rules(
            "retention.xlsx",
            "Retention",
        )


@pytest.mark.parametrize(
    "max_rows",
    [
        0,
        -1,
        True,
    ],
)
def test_reader_rejects_invalid_max_rows(
    max_rows: int,
) -> None:
    # when / then
    with pytest.raises(
        ValueError,
        match="max_rows must be a positive integer",
    ):
        ExcelRetentionRulesReader(
            MagicMock(),
            max_rows=max_rows,
        )
