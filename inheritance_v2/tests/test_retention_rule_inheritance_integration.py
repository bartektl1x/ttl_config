from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import Row, SparkSession

from inheritance_v2.retention_rule_inheritance import RetentionRuleInheritanceResolver
from ttl_config.retention_rules import RetentionRule, TableName


class _SparkTables:
    def __init__(self, spark: SparkSession, lineage: object, updates: object) -> None:
        self._spark = spark
        self._tables = {
            "system.access.table_lineage": lineage,
            "system.lakeflow.pipeline_update_timeline": updates,
        }

    def table(self, name: str):
        return self._tables[name]

    def createDataFrame(self, *args, **kwargs):
        return self._spark.createDataFrame(*args, **kwargs)


def _edge(
    source: str,
    target: str,
    *,
    source_type: str = "STREAMING_TABLE",
    target_type: str = "STREAMING_TABLE",
    source_catalog: str = "dev_silver",
    target_catalog: str = "dev_silver",
    update: str = "current",
    metastore: str = "metastore-1",
    direct: bool = True,
    pipeline: str = "pipeline-1",
) -> Row:
    return Row(
        metastore_id=metastore,
        direct_access=direct,
        source_type=source_type,
        target_type=target_type,
        source_table_catalog=source_catalog,
        source_table_schema="RETENTION",
        source_table_name=source,
        target_table_catalog=target_catalog,
        target_table_schema="RETENTION",
        target_table_name=target,
        workspace_id="workspace-1",
        entity_metadata=Row(
            dlt_pipeline_info=Row(
                dlt_pipeline_id=pipeline,
                dlt_update_id=update,
            )
        ),
    )


@pytest.fixture(scope="module")
def resolver(spark: SparkSession) -> RetentionRuleInheritanceResolver:
    base_time = datetime(2026, 1, 1, 12, tzinfo=UTC)
    lineage = spark.createDataFrame(
        [
            _edge("A", "stale", update="old", source_catalog="dev_bronze"),
            _edge(
                "A",
                "B",
                source_type="TABLE",
                source_catalog="DEV_BronZe",
                target_catalog="DEV_SilVer",
            ),
            _edge(
                "A",
                "bronze_copy",
                source_type="TABLE",
                source_catalog="DEV_BronZe",
                target_catalog="dev_bronze",
            ),
            _edge("B", "V", target_type="VIEW"),
            _edge("V", "C", source_type="VIEW"),
            _edge("C", "D", target_type="TABLE", pipeline="pipeline-2"),
            _edge("A", "failed", update="failed", source_catalog="dev_bronze"),
            _edge("A", "validate", update="validate", source_catalog="dev_bronze"),
            _edge("A", "indirect", direct=False, source_catalog="dev_bronze"),
            _edge("A", "path", source_type="PATH", source_catalog="dev_bronze"),
            _edge("C", "mv", target_type="MATERIALIZED_VIEW"),
            _edge("mv", "behind_mv", source_type="MATERIALIZED_VIEW"),
            _edge("C", "gold", target_catalog="dev_gold"),
            _edge("shared", "B", source_catalog="shared_reference"),
            _edge("C", "shared", target_catalog="shared_reference"),
            _edge(
                "A", "other_metastore", source_catalog="dev_bronze", metastore="other"
            ),
        ]
    )
    updates = spark.createDataFrame(
        [
            Row(
                workspace_id="workspace-1",
                pipeline_id="pipeline-1",
                update_id=update,
                period_end_time=base_time + timedelta(hours=hours),
                result_state=result,
                update_type=kind,
            )
            for update, hours, result, kind in (
                ("old", 0, "COMPLETED", "REFRESH"),
                ("current", 1, "COMPLETED", "FULL_REFRESH"),
                ("failed", 2, "FAILED", "REFRESH"),
                ("validate", 3, "COMPLETED", "VALIDATE"),
            )
        ]
        + [
            Row(
                workspace_id="workspace-1",
                pipeline_id="pipeline-2",
                update_id="current",
                period_end_time=base_time - timedelta(hours=1),
                result_state="COMPLETED",
                update_type="REFRESH",
            )
        ]
    )
    return RetentionRuleInheritanceResolver(_SparkTables(spark, lineage, updates))


def _table(name: str, catalog: str = "dev_silver") -> TableName:
    return TableName(catalog=catalog, schema="retention", table=name)


def test_current_dependencies_filter_update_metastore_types_and_catalogs(
    resolver: RetentionRuleInheritanceResolver,
) -> None:
    dependencies = resolver._read_current_dependencies().collect()

    assert {
        (row["source_table"], row["target_table"], row["target_type"])
        for row in dependencies
    } == {
        ("a", "b", "STREAMING_TABLE"),
        ("a", "bronze_copy", "STREAMING_TABLE"),
        ("b", "v", "VIEW"),
        ("v", "c", "STREAMING_TABLE"),
        ("c", "d", "TABLE"),
    }
    assert {
        (row["source_catalog"], row["target_catalog"])
        for row in dependencies
        if row["source_table"] == "a"
    } == {("dev_bronze", "dev_silver"), ("dev_bronze", "dev_bronze")}


def test_reachable_frontier_propagates_through_view_only(
    resolver: RetentionRuleInheritanceResolver,
) -> None:
    explicit = (
        RetentionRule(
            table_name=_table("a", "dev_bronze"),
            time_column="event_time",
            expiration_days=30,
        ),
    )

    rules = resolver.inherit_retention_rules(explicit)

    assert rules == (
        *explicit,
        RetentionRule(
            table_name=_table("bronze_copy", "dev_bronze"),
            time_column="event_time",
            expiration_days=30,
        ),
        *(
            RetentionRule(
                table_name=_table(name),
                time_column="event_time",
                expiration_days=30,
            )
            for name in "bcd"
        ),
    )


def test_unreachable_sources_do_not_enter_collected_graph(
    resolver: RetentionRuleInheritanceResolver,
) -> None:
    explicit = (
        RetentionRule(
            table_name=_table("d"),
            time_column="event_time",
            expiration_days=30,
        ),
    )

    assert resolver._read_lineage(explicit) == ({}, set())
