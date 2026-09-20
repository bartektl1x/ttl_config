import pytest
from pydantic import ValidationError

from retention_config.retention_rules import (
    MAX_EXPIRATION_DAYS,
    RetentionRule,
    SchemaName,
    TableName,
)


def test_table_name_normalizes_identity() -> None:
    # given
    table = TableName(
        catalog="  OPS  ",
        schema="  RETENTION  ",
        table="  CONFIG  ",
    )

    # when
    identity = (
        table.catalog,
        table.schema_name,
        table.table,
    )

    # then
    assert identity == (
        "ops",
        "retention",
        "config",
    )


def test_table_name_escapes_backticks_in_sql_identifier() -> None:
    # given
    table = TableName(
        catalog="ops`prod",
        schema="retention",
        table="config",
    )

    # when / then
    assert table.sql_identifier == "`ops``prod`.`retention`.`config`"


@pytest.mark.parametrize(
    "schema_input",
    [
        {"catalog": "ops", "schema": "retention"},
        {"catalog": "ops", "schema_name": "retention"},
    ],
)
def test_schema_name_accepts_alias_and_field_name(
    schema_input: dict[str, str],
) -> None:
    # given
    schema = SchemaName(**schema_input)

    # when
    schema_name = schema.schema_name

    # then
    assert schema_name == "retention"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("catalog", "bad.name"),
        ("schema_name", "bad schema"),
        ("table", "bad/name"),
        ("table", "bad\nname"),
    ],
)
def test_table_name_rejects_invalid_name_components(
    field: str,
    value: str,
) -> None:
    # given
    table_input = {
        "catalog": "ops",
        "schema_name": "retention",
        "table": "retention_config",
    }
    table_input[field] = value

    # when / then
    with pytest.raises(ValidationError):
        TableName(**table_input)


@pytest.mark.parametrize(
    "time_column",
    [
        "payload.event_time",
        "event\ntime",
    ],
)
def test_retention_rule_rejects_invalid_time_column(
    time_column: str,
) -> None:
    # given
    table = TableName(
        catalog="ops",
        schema="retention",
        table="events",
    )

    # when / then
    with pytest.raises(ValidationError):
        RetentionRule(
            table_name=table,
            time_column=time_column,
            expiration_days=30,
        )


@pytest.mark.parametrize(
    "expiration_days",
    [
        0,
        MAX_EXPIRATION_DAYS,
    ],
)
def test_retention_rule_accepts_expiration_boundaries(
    expiration_days: int,
) -> None:
    # given
    table = TableName(
        catalog="ops",
        schema="retention",
        table="events",
    )

    # when
    rule = RetentionRule(
        table_name=table,
        time_column="event_time",
        expiration_days=expiration_days,
    )

    # then
    assert rule.expiration_days == expiration_days


@pytest.mark.parametrize(
    "expiration_days",
    [
        -1,
        MAX_EXPIRATION_DAYS + 1,
    ],
)
def test_retention_rule_rejects_expiration_outside_boundaries(
    expiration_days: int,
) -> None:
    # given
    table = TableName(
        catalog="ops",
        schema="retention",
        table="events",
    )

    # when / then
    with pytest.raises(ValidationError):
        RetentionRule(
            table_name=table,
            time_column="event_time",
            expiration_days=expiration_days,
        )
