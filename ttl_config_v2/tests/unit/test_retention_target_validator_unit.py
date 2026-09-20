from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row

from retention_config.retention_rules import RetentionRule, TableName
from retention_config.retention_target_validator import RetentionTargetValidator


def _rule(
    table: str = "orders",
) -> RetentionRule:
    return RetentionRule(
        table_name=TableName(
            catalog="main",
            schema="sales",
            table=table,
        ),
        time_column="event_time",
        expiration_days=30,
    )


def _target_metadata(**overrides: object) -> Row:
    values = {
        "catalog": "main",
        "schema": "sales",
        "table": "orders",
        "time_column": "event_time",
        "table_type": "MANAGED",
        "data_source_format": "DELTA",
        "actual_time_column": "event_time",
        "time_column_type": "TIMESTAMP",
    }
    values.update(overrides)
    return Row(**values)


def _validator_with_metadata(
    monkeypatch: pytest.MonkeyPatch,
    metadata: list[Row],
) -> RetentionTargetValidator:
    validator = RetentionTargetValidator(MagicMock())
    monkeypatch.setattr(
        validator,
        "_read_target_metadata",
        lambda _: metadata,
    )
    return validator


@pytest.mark.parametrize(
    ("table_type", "data_source_format"),
    [
        ("MANAGED", "DELTA"),
        ("MANAGED", "ICEBERG"),
        ("STREAMING_TABLE", "DELTA"),
    ],
)
def test_validate_retention_targets_accepts_supported_table_formats(
    monkeypatch: pytest.MonkeyPatch,
    table_type: str,
    data_source_format: str,
) -> None:
    # given
    validator = _validator_with_metadata(
        monkeypatch,
        [
            _target_metadata(
                table_type=table_type,
                data_source_format=data_source_format,
            )
        ],
    )

    # when / then
    validator.validate_retention_targets((_rule(),))


@pytest.mark.parametrize(
    "time_column_type",
    [
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
    ],
)
def test_validate_retention_targets_accepts_supported_time_column_types(
    monkeypatch: pytest.MonkeyPatch,
    time_column_type: str,
) -> None:
    # given
    validator = _validator_with_metadata(
        monkeypatch,
        [_target_metadata(time_column_type=time_column_type)],
    )

    # when / then
    validator.validate_retention_targets((_rule(),))


def test_validate_retention_targets_rejects_missing_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    metadata = [
        _target_metadata(
            table="orders",
            table_type=None,
            data_source_format=None,
            actual_time_column=None,
            time_column_type=None,
        ),
        _target_metadata(
            table="customers",
            table_type=None,
            data_source_format=None,
            actual_time_column=None,
            time_column_type=None,
        ),
    ]
    validator = _validator_with_metadata(monkeypatch, metadata)

    # when / then
    with pytest.raises(ValueError) as error:
        validator.validate_retention_targets(
            (
                _rule("orders"),
                _rule("customers"),
            )
        )

    assert "`main`.`sales`.`customers`" in str(error.value)
    assert "`main`.`sales`.`orders`" in str(error.value)


@pytest.mark.parametrize(
    ("table_type", "data_source_format"),
    [
        ("EXTERNAL", "DELTA"),
        ("MANAGED", "PARQUET"),
        ("STREAMING_TABLE", "ICEBERG"),
    ],
)
def test_validate_retention_targets_rejects_unsupported_table_formats(
    monkeypatch: pytest.MonkeyPatch,
    table_type: str,
    data_source_format: str,
) -> None:
    # given
    validator = _validator_with_metadata(
        monkeypatch,
        [
            _target_metadata(
                table_type=table_type,
                data_source_format=data_source_format,
            )
        ],
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="Unsupported retention targets",
    ):
        validator.validate_retention_targets((_rule(),))


def test_validate_retention_targets_rejects_missing_time_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    validator = _validator_with_metadata(
        monkeypatch,
        [
            _target_metadata(
                actual_time_column=None,
                time_column_type=None,
            )
        ],
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="Time columns do not exist",
    ):
        validator.validate_retention_targets((_rule(),))


def test_validate_retention_targets_rejects_unsupported_time_column_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    validator = _validator_with_metadata(
        monkeypatch,
        [_target_metadata(time_column_type="STRING")],
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="Time columns must be DATE, TIMESTAMP, or TIMESTAMP_NTZ",
    ):
        validator.validate_retention_targets((_rule(),))
