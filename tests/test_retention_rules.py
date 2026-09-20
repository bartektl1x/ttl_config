import pytest
from pydantic import ValidationError

from ttl_config.retention_rules import RetentionRule, TableName


def test_table_name_is_canonical_and_sql_quoted() -> None:
    table = TableName(
        catalog="Business_Catalog",
        schema="Gold",
        table="Orders",
    )

    assert table.catalog == "business_catalog"
    assert table.schema_name == "gold"
    assert table.table == "orders"
    assert table.sql_identifier == ".".join(
        f"{chr(96)}{part}{chr(96)}"
        for part in ("business_catalog", "gold", "orders")
    )


def test_retention_rule_accepts_zero_days() -> None:
    rule = RetentionRule(
        table_name=TableName(catalog="catalog", schema="schema", table="table"),
        time_column="event_date",
        expiration_days=0,
    )

    assert rule.expiration_days == 0


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {
                "table_name": {
                    "catalog": "catalog",
                    "schema": "schema",
                    "table": "table",
                },
                "time_column": "nested.field",
                "expiration_days": 30,
            },
            "top-level column",
        ),
        (
            {
                "table_name": {
                    "catalog": "catalog",
                    "schema": "schema",
                    "table": "table",
                },
                "time_column": "event_date",
                "expiration_days": -1,
            },
            "greater than or equal to 0",
        ),
    ],
)
def test_invalid_retention_rule_is_rejected(
    payload: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        RetentionRule(**payload)
