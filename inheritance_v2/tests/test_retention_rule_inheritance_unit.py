import logging
from unittest.mock import MagicMock

import pytest
from pyspark.errors import PySparkException

from inheritance_v2.retention_rule_inheritance import RetentionRuleInheritanceResolver
from ttl_config.retention_rules import RetentionRule, TableName


def _table(name: str, catalog: str = "dev_silver") -> TableName:
    return TableName(catalog=catalog, schema="retention", table=name)


def _rule(
    name: str,
    days: int,
    column: str = "event_time",
    catalog: str = "dev_silver",
) -> RetentionRule:
    return RetentionRule(
        table_name=_table(name, catalog),
        time_column=column,
        expiration_days=days,
    )


def _resolver(
    monkeypatch: pytest.MonkeyPatch,
    lineage: dict[TableName, set[TableName]],
    views: set[TableName] | None = None,
) -> RetentionRuleInheritanceResolver:
    resolver = RetentionRuleInheritanceResolver(MagicMock())
    monkeypatch.setattr(resolver, "_read_lineage", lambda _: (lineage, views or set()))
    return resolver


def test_transitive_inheritance_keeps_column_and_expiration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a = _table("a", "dev_bronze")
    b = _table("b")
    c = _table("c")
    resolver = _resolver(monkeypatch, {a: {b}, b: {c}})

    rules = resolver.inherit_retention_rules((_rule("a", 30, catalog="dev_bronze"),))

    assert rules == (
        _rule("a", 30, catalog="dev_bronze"),
        _rule("b", 30),
        _rule("c", 30),
    )


def test_explicit_override_propagates_its_own_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, b, c = (_table(name) for name in "abc")
    explicit = (_rule("a", 30), _rule("b", 90))
    resolver = _resolver(monkeypatch, {a: {b}, b: {c}})

    assert resolver.inherit_retention_rules(explicit) == (*explicit, _rule("c", 90))


def test_equal_candidates_collapse_even_when_column_case_differs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, b, c = (_table(name) for name in "abc")
    resolver = _resolver(monkeypatch, {a: {c}, b: {c}})
    explicit = (_rule("a", 30, "Event_Time"), _rule("b", 30, "event_time"))

    rules = resolver.inherit_retention_rules(explicit)
    reversed_resolver = _resolver(monkeypatch, {b: {c}, a: {c}})
    reversed_rules = reversed_resolver.inherit_retention_rules(
        tuple(reversed(explicit))
    )

    expected = RetentionRule(table_name=c, time_column="Event_Time", expiration_days=30)
    assert rules == (*explicit, expected)
    assert reversed_rules == (*reversed(explicit), expected)


@pytest.mark.parametrize(
    ("second_column", "second_days"),
    [("event_time", 90), ("created_at", 30)],
)
def test_conflicting_candidates_stop_inheritance(
    monkeypatch: pytest.MonkeyPatch,
    second_column: str,
    second_days: int,
) -> None:
    a, b, c = (_table(name) for name in "abc")
    explicit = (_rule("a", 30), _rule("b", second_days, second_column))
    resolver = _resolver(monkeypatch, {a: {c}, b: {c}})

    assert resolver.inherit_retention_rules(explicit) == explicit


def test_ambiguous_intermediate_does_not_propagate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, b, c, x = (_table(name) for name in "abcx")
    explicit = (_rule("a", 30), _rule("x", 90))
    resolver = _resolver(monkeypatch, {a: {b}, x: {b}, b: {c}})

    assert resolver.inherit_retention_rules(explicit) == explicit


def test_independent_path_survives_ambiguous_intermediate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, b, c, d, x = (_table(name) for name in "abcdx")
    explicit = (_rule("a", 30), _rule("x", 90), _rule("d", 30))
    resolver = _resolver(monkeypatch, {a: {b}, x: {b}, b: {c}, d: {c}})

    assert resolver.inherit_retention_rules(explicit) == (*explicit, _rule("c", 30))


def test_view_transmits_policy_but_is_not_retention_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a, view, b = (_table(name) for name in ("a", "orders_view", "b"))
    explicit = (_rule("a", 30),)
    resolver = _resolver(monkeypatch, {a: {view}, view: {b}}, {view})

    assert resolver.inherit_retention_rules(explicit) == (*explicit, _rule("b", 30))


def test_spark_failure_returns_explicit_rules(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    resolver = RetentionRuleInheritanceResolver(MagicMock())
    explicit = (_rule("a", 30),)

    def fail(*args: object) -> None:
        raise PySparkException("lineage unavailable")

    monkeypatch.setattr(resolver, "_read_current_dependencies", lambda: MagicMock())
    monkeypatch.setattr(resolver, "_read_frontier_dependencies", fail)

    with caplog.at_level(logging.WARNING):
        assert resolver.inherit_retention_rules(explicit) == explicit

    assert "Retention lineage read failed" in caplog.text


def test_budget_overflow_returns_explicit_rules(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    resolver = RetentionRuleInheritanceResolver(MagicMock(), max_mappings=1)
    explicit = (_rule("a", 30),)
    monkeypatch.setattr(resolver, "_read_current_dependencies", lambda: MagicMock())
    monkeypatch.setattr(
        resolver, "_read_frontier_dependencies", lambda *_: [object(), object()]
    )

    with caplog.at_level(logging.WARNING):
        assert resolver.inherit_retention_rules(explicit) == explicit

    assert "Retention lineage exceeds max_mappings=1" in caplog.text


@pytest.mark.parametrize("max_mappings", [0, -1, True])
def test_invalid_budget_is_rejected(max_mappings: int) -> None:
    with pytest.raises(ValueError, match="max_mappings must be a positive integer"):
        RetentionRuleInheritanceResolver(MagicMock(), max_mappings=max_mappings)
