from datetime import datetime, timedelta

from pyspark.sql import Row, SparkSession

from retention_config.retention_rule_inheritance import RetentionRuleInheritanceResolver
from retention_config.retention_rules import RetentionRule, TableName


class _SparkStub:
    def __init__(
        self,
        spark: SparkSession,
        tables: dict[str, object],
    ) -> None:
        self._spark = spark
        self._tables = tables

    def table(self, name: str):
        return self._tables[name]

    def createDataFrame(self, *args, **kwargs):
        return self._spark.createDataFrame(*args, **kwargs)


def _table(name: str) -> TableName:
    return TableName(
        catalog="main",
        schema="retention",
        table=name,
    )


def test_read_current_mappings_uses_latest_completed_refresh(
    spark: SparkSession,
) -> None:
    # given
    metastore_id = spark.sql(
        "SELECT substring_index(current_metastore(), ':', -1) AS metastore_id"
    ).first()["metastore_id"]
    base_time = datetime(2026, 1, 1, 12, 0, 0)

    lineage_rows = [
        Row(
            metastore_id=metastore_id,
            direct_access=True,
            source_type="TABLE",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="A",
            source_column_name="EVENT_TIME",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="B",
            target_column_name="old_time",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-1",
                    dlt_update_id="update-1",
                )
            ),
        ),
        Row(
            metastore_id=metastore_id,
            direct_access=True,
            source_type="TABLE",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="A",
            source_column_name="EVENT_TIME",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="B",
            target_column_name="current_time",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-1",
                    dlt_update_id="update-2",
                )
            ),
        ),
        Row(
            metastore_id=metastore_id,
            direct_access=True,
            source_type="TABLE",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="A",
            source_column_name="EVENT_TIME",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="B",
            target_column_name="failed_time",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-1",
                    dlt_update_id="update-3",
                )
            ),
        ),
        Row(
            metastore_id=metastore_id,
            direct_access=False,
            source_type="TABLE",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="A",
            source_column_name="EVENT_TIME",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="B",
            target_column_name="indirect_time",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-1",
                    dlt_update_id="update-2",
                )
            ),
        ),
        Row(
            metastore_id=metastore_id,
            direct_access=True,
            source_type="TABLE",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="A",
            source_column_name="EVENT_TIME",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="B",
            target_column_name="payload.current_time",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-1",
                    dlt_update_id="update-2",
                )
            ),
        ),
        Row(
            metastore_id=metastore_id,
            direct_access=True,
            source_type="VIEW",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="A",
            source_column_name="EVENT_TIME",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="B",
            target_column_name="view_time",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-1",
                    dlt_update_id="update-2",
                )
            ),
        ),
        Row(
            metastore_id=metastore_id,
            direct_access=True,
            source_type="STREAMING_TABLE",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="D",
            source_column_name="CREATED_AT",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="E",
            target_column_name="created_at",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-2",
                    dlt_update_id="update-5",
                )
            ),
        ),
        Row(
            metastore_id="another-metastore",
            direct_access=True,
            source_type="TABLE",
            target_type="TABLE",
            source_table_catalog="MAIN",
            source_table_schema="RETENTION",
            source_table_name="A",
            source_column_name="EVENT_TIME",
            target_table_catalog="MAIN",
            target_table_schema="RETENTION",
            target_table_name="B",
            target_column_name="other_metastore_time",
            workspace_id="workspace-1",
            entity_metadata=Row(
                dlt_pipeline_info=Row(
                    dlt_pipeline_id="pipeline-1",
                    dlt_update_id="update-2",
                )
            ),
        ),
    ]

    update_rows = [
        Row(
            workspace_id="workspace-1",
            pipeline_id="pipeline-1",
            update_id="update-1",
            period_end_time=base_time,
            result_state="COMPLETED",
            update_type="REFRESH",
        ),
        Row(
            workspace_id="workspace-1",
            pipeline_id="pipeline-1",
            update_id="update-2",
            period_end_time=base_time + timedelta(hours=1),
            result_state="COMPLETED",
            update_type="FULL_REFRESH",
        ),
        Row(
            workspace_id="workspace-1",
            pipeline_id="pipeline-1",
            update_id="update-3",
            period_end_time=base_time + timedelta(hours=2),
            result_state="FAILED",
            update_type="REFRESH",
        ),
        Row(
            workspace_id="workspace-1",
            pipeline_id="pipeline-2",
            update_id="update-5",
            period_end_time=base_time - timedelta(hours=1),
            result_state="COMPLETED",
            update_type="REFRESH",
        ),
        Row(
            workspace_id="workspace-1",
            pipeline_id="pipeline-1",
            update_id="update-4",
            period_end_time=base_time + timedelta(hours=3),
            result_state="COMPLETED",
            update_type="VALIDATE",
        ),
    ]

    lineage = spark.createDataFrame(lineage_rows)
    updates = spark.createDataFrame(update_rows)
    spark_stub = _SparkStub(
        spark,
        {
            "system.access.column_lineage": lineage,
            "system.lakeflow.pipeline_update_timeline": updates,
        },
    )
    resolver = RetentionRuleInheritanceResolver(spark_stub)

    # when
    mappings = [
        row.asDict()
        for row in resolver._read_current_mappings().collect()
    ]

    # then
    assert sorted(
        mappings,
        key=lambda mapping: mapping["source_table"],
    ) == [
        {
            "source_catalog": "main",
            "source_schema": "retention",
            "source_table": "a",
            "source_time_column": "event_time",
            "target_catalog": "main",
            "target_schema": "retention",
            "target_table": "b",
            "target_time_column": "current_time",
        },
        {
            "source_catalog": "main",
            "source_schema": "retention",
            "source_table": "d",
            "source_time_column": "created_at",
            "target_catalog": "main",
            "target_schema": "retention",
            "target_table": "e",
            "target_time_column": "created_at",
        },
    ]


def test_collect_reachable_lineage_uses_real_spark_frontier_queries(
    spark: SparkSession,
) -> None:
    # given
    current_mappings = spark.createDataFrame(
        [
            ("main", "retention", "a", "event_time", "main", "retention", "b", "EventTime"),
            ("main", "retention", "a", "event_time", "main", "retention", "b", "EventTime"),
            ("main", "retention", "b", "eventtime", "main", "retention", "c", "created_at"),
            ("main", "retention", "x", "event_time", "main", "retention", "y", "event_time"),
            (
                "main",
                "retention",
                "a",
                "event_time",
                "bad.name",
                "retention",
                "ignored",
                "event_time",
            ),
        ],
        schema=(
            "source_catalog STRING, "
            "source_schema STRING, "
            "source_table STRING, "
            "source_time_column STRING, "
            "target_catalog STRING, "
            "target_schema STRING, "
            "target_table STRING, "
            "target_time_column STRING"
        ),
    )
    resolver = RetentionRuleInheritanceResolver(
        _SparkStub(spark, {}),
    )
    explicit_rules = (
        RetentionRule(
            table_name=_table("a"),
            time_column="event_time",
            expiration_days=30,
        ),
    )

    # when
    lineage = resolver._collect_reachable_lineage(
        explicit_rules,
        current_mappings,
    )

    # then
    assert dict(lineage) == {
        (_table("a"), "event_time"): {
            (_table("b"), "EventTime"),
        },
        (_table("b"), "eventtime"): {
            (_table("c"), "created_at"),
        },
    }
