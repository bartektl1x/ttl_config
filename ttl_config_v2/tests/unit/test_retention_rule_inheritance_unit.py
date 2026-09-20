import logging
from unittest.mock import MagicMock

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import Row

from retention_config.retention_rule_inheritance import RetentionRuleInheritanceResolver
from retention_config.retention_rules import RetentionRule, TableName


def _table(name: str) -> TableName:
    return TableName(
        catalog="main",
        schema="retention",
        table=name,
    )


def _rule(
    table: str,
    expiration_days: int,
    time_column: str = "event_time",
) -> RetentionRule:
    return RetentionRule(
        table_name=_table(table),
        time_column=time_column,
        expiration_days=expiration_days,
    )


def _column(
    table: str,
    time_column: str = "event_time",
) -> tuple[TableName, str]:
    return _table(table), time_column


def _resolver_with_lineage(
    monkeypatch: pytest.MonkeyPatch,
    lineage: dict[
        tuple[TableName, str],
        set[tuple[TableName, str]],
    ],
) -> RetentionRuleInheritanceResolver:
    resolver = RetentionRuleInheritanceResolver(MagicMock())
    monkeypatch.setattr(
        resolver,
        "_read_lineage",
        lambda _: lineage,
    )
    return resolver


def test_inherit_retention_rules_propagates_transitively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    explicit_rules = (_rule("a", 30),)
    lineage = {
        _column("a"): {_column("b")},
        _column("b"): {_column("c")},
    }
    resolver = _resolver_with_lineage(monkeypatch, lineage)

    # when
    rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == (
        _rule("a", 30),
        _rule("b", 30),
        _rule("c", 30),
    )


def test_inherit_retention_rules_explicit_rule_overrides_and_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    explicit_rules = (
        _rule("a", 30),
        _rule("b", 90),
    )
    lineage = {
        _column("a"): {_column("b")},
        _column("b"): {_column("c")},
    }
    resolver = _resolver_with_lineage(monkeypatch, lineage)

    # when
    rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == (
        _rule("a", 30),
        _rule("b", 90),
        _rule("c", 90),
    )


def test_inherit_retention_rules_collapses_equal_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    explicit_rules = (
        _rule("a", 30),
        _rule("b", 30),
    )
    lineage = {
        _column("a"): {_column("c", "EventTime")},
        _column("b"): {_column("c", "eventtime")},
    }
    resolver = _resolver_with_lineage(monkeypatch, lineage)

    # when
    rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert len(rules) == 3
    assert rules[-1].table_name == _table("c")
    assert rules[-1].time_column.lower() == "eventtime"
    assert rules[-1].expiration_days == 30


def test_inherit_retention_rules_rejects_conflicting_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    explicit_rules = (
        _rule("a", 30),
        _rule("b", 90),
    )
    lineage = {
        _column("a"): {_column("c")},
        _column("b"): {_column("c")},
    }
    resolver = _resolver_with_lineage(monkeypatch, lineage)

    # when
    rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == explicit_rules


def test_inherit_retention_rules_stops_at_ambiguous_intermediate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    explicit_rules = (
        _rule("a", 30),
        _rule("x", 90),
    )
    lineage = {
        _column("a"): {_column("b")},
        _column("x"): {_column("b")},
        _column("b"): {_column("c")},
    }
    resolver = _resolver_with_lineage(monkeypatch, lineage)

    # when
    rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == explicit_rules


def test_inherit_retention_rules_keeps_independent_path_after_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    explicit_rules = (
        _rule("a", 30),
        _rule("x", 90),
        _rule("d", 30),
    )
    lineage = {
        _column("a"): {_column("b")},
        _column("x"): {_column("b")},
        _column("b"): {_column("c")},
        _column("d"): {_column("c")},
    }
    resolver = _resolver_with_lineage(monkeypatch, lineage)

    # when
    rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == (
        *explicit_rules,
        _rule("c", 30),
    )


def test_inherit_retention_rules_skips_inferred_rule_if_validation_changes_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    explicit_rules = (_rule("a", 30),)
    lineage = {
        _column("a"): {_column("b", " event_time ")},
    }
    resolver = _resolver_with_lineage(monkeypatch, lineage)

    # when
    rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == explicit_rules


def test_inherit_retention_rules_falls_back_on_spark_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # given
    explicit_rules = (_rule("a", 30),)
    resolver = RetentionRuleInheritanceResolver(MagicMock())

    def raise_spark_error() -> None:
        raise PySparkException("lineage unavailable")

    monkeypatch.setattr(
        resolver,
        "_read_current_mappings",
        raise_spark_error,
    )

    # when
    with caplog.at_level(logging.WARNING):
        rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == explicit_rules
    assert "Retention lineage read failed" in caplog.text


def test_inherit_retention_rules_falls_back_when_mapping_budget_is_exceeded(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # given
    explicit_rules = (_rule("a", 30),)
    resolver = RetentionRuleInheritanceResolver(
        MagicMock(),
        max_mappings=1,
    )
    monkeypatch.setattr(
        resolver,
        "_read_current_mappings",
        lambda: MagicMock(),
    )
    monkeypatch.setattr(
        resolver,
        "_read_frontier_mappings",
        lambda *_: [Row(), Row()],
    )

    # when
    with caplog.at_level(logging.WARNING):
        rules = resolver.inherit_retention_rules(explicit_rules)

    # then
    assert rules == explicit_rules
    assert "Retention lineage exceeds max_mappings=1" in caplog.text


@pytest.mark.parametrize(
    "max_mappings",
    [
        0,
        -1,
        True,
    ],
)
def test_resolver_rejects_invalid_max_mappings(
    max_mappings: int,
) -> None:
    # when / then
    with pytest.raises(
        ValueError,
        match="max_mappings must be a positive integer",
    ):
        RetentionRuleInheritanceResolver(
            MagicMock(),
            max_mappings=max_mappings,
        )
