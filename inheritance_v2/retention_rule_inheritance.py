from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from graphlib import TopologicalSorter
from typing import TYPE_CHECKING

from pydantic import ValidationError
from pyspark.errors import PySparkException
from pyspark.sql import Window
from pyspark.sql import functions as F

from ttl_config.retention_rules import RetentionRule, RetentionRules, TableName

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

type _TableLineage = dict[TableName, set[TableName]]

_LOGGER = logging.getLogger(__name__)


class RetentionRuleInheritanceResolver:
    """Inherit policies along current, direct Bronze/Silver table lineage.

    The retention column name and expiration stay unchanged. Views carry a
    policy downstream without becoming retention targets. Explicit rules win.
    """

    _SOURCE_COLUMNS = ("source_catalog", "source_schema", "source_table")
    _UPDATE_COLUMNS = ("workspace_id", "pipeline_id", "update_id")
    _LINEAGE_TYPES = ("TABLE", "STREAMING_TABLE", "VIEW")

    def __init__(
        self,
        spark: SparkSession,
        *,
        max_mappings: int = 50_000,
    ) -> None:
        if type(max_mappings) is not int or max_mappings < 1:
            raise ValueError("max_mappings must be a positive integer")

        self._spark = spark
        self._max_mappings = max_mappings

    def inherit_retention_rules(
        self,
        explicit_rules: RetentionRules,
    ) -> RetentionRules:
        """Add unambiguous downstream policies; keep explicit rules authoritative."""
        lineage, views = self._read_lineage(explicit_rules)

        if not lineage:
            return explicit_rules

        inherited_rules = self._resolve_inherited_rules(
            explicit_rules,
            lineage,
            views,
        )
        return (*explicit_rules, *inherited_rules)

    def _read_lineage(
        self,
        explicit_rules: RetentionRules,
    ) -> tuple[_TableLineage, set[TableName]]:
        """Skip optional enrichment when Spark cannot read lineage."""
        try:
            dependencies = self._read_current_dependencies()
            return self._collect_reachable_lineage(explicit_rules, dependencies)
        except PySparkException:
            _LOGGER.warning(
                "Retention lineage read failed; keeping explicit rules",
                exc_info=True,
            )
            return {}, set()

    def _read_current_dependencies(self) -> DataFrame:
        """Keep direct edges from the latest successful update per pipeline."""
        dependencies = self._read_direct_table_dependencies()
        latest_updates = self._read_latest_completed_updates()

        return dependencies.join(
            latest_updates,
            on=list(self._UPDATE_COLUMNS),
            how="left_semi",
        ).select(
            *self._SOURCE_COLUMNS,
            "target_catalog",
            "target_schema",
            "target_table",
            "target_type",
        )

    def _read_direct_table_dependencies(self) -> DataFrame:
        """Read supported direct table/view edges in Bronze/Silver catalogs."""
        source_catalog = F.lower(F.col("source_table_catalog"))
        target_catalog = F.lower(F.col("target_table_catalog"))

        return (
            self._spark.table("system.access.table_lineage")
            # The lineage table stores the UUID; current_metastore() includes
            # its cloud and region prefix.
            .where("metastore_id = substring_index(current_metastore(), ':', -1)")
            .where(
                (F.col("direct_access") == F.lit(True))
                & F.col("source_type").isin(*self._LINEAGE_TYPES)
                & F.col("target_type").isin(*self._LINEAGE_TYPES)
                & (
                    source_catalog.contains("bronze")
                    | source_catalog.contains("silver")
                )
                & (
                    target_catalog.contains("bronze")
                    | target_catalog.contains("silver")
                )
            )
            .select(
                source_catalog.alias("source_catalog"),
                F.lower("source_table_schema").alias("source_schema"),
                F.lower("source_table_name").alias("source_table"),
                target_catalog.alias("target_catalog"),
                F.lower("target_table_schema").alias("target_schema"),
                F.lower("target_table_name").alias("target_table"),
                "target_type",
                "workspace_id",
                F.col("entity_metadata.dlt_pipeline_info.dlt_pipeline_id").alias(
                    "pipeline_id"
                ),
                F.col("entity_metadata.dlt_pipeline_info.dlt_update_id").alias(
                    "update_id"
                ),
            )
        )

    def _read_latest_completed_updates(self) -> DataFrame:
        """Select the latest completed refresh for each workspace/pipeline."""
        latest_first = Window.partitionBy(
            "workspace_id",
            "pipeline_id",
        ).orderBy(F.desc("period_end_time"))

        # Choose the latest update before joining dependencies: an absent edge
        # in a newer update must not expose an older, removed dependency.
        return (
            self._spark.table("system.lakeflow.pipeline_update_timeline")
            .where(
                (F.col("result_state") == "COMPLETED")
                & F.col("update_type").isin("REFRESH", "FULL_REFRESH")
            )
            .select(*self._UPDATE_COLUMNS, "period_end_time")
            .withColumn("update_rank", F.row_number().over(latest_first))
            .where(F.col("update_rank") == 1)
            .select(*self._UPDATE_COLUMNS)
        )

    def _collect_reachable_lineage(
        self,
        explicit_rules: RetentionRules,
        dependencies: DataFrame,
    ) -> tuple[_TableLineage, set[TableName]]:
        """Traverse downstream from explicit tables within one collection budget."""
        frontier = {rule.table_name for rule in explicit_rules}
        queried_tables: set[TableName] = set()
        lineage: _TableLineage = defaultdict(set)
        views: set[TableName] = set()
        collected_count = 0

        while frontier:
            queried_tables.update(frontier)
            remaining = self._max_mappings - collected_count
            rows = self._read_frontier_dependencies(dependencies, frontier, remaining)

            if len(rows) > remaining:
                _LOGGER.warning(
                    "Retention lineage exceeds max_mappings=%s; skipping inheritance",
                    self._max_mappings,
                )
                return {}, set()

            collected_count += len(rows)
            next_frontier: set[TableName] = set()

            for row in rows:
                try:
                    source = TableName(
                        catalog=row["source_catalog"],
                        schema=row["source_schema"],
                        table=row["source_table"],
                    )
                    target = TableName(
                        catalog=row["target_catalog"],
                        schema=row["target_schema"],
                        table=row["target_table"],
                    )
                except ValidationError:
                    continue

                lineage[source].add(target)
                if row["target_type"] == "VIEW":
                    views.add(target)

                if target not in queried_tables:
                    next_frontier.add(target)

            frontier = next_frontier

        return lineage, views

    def _read_frontier_dependencies(
        self,
        dependencies: DataFrame,
        frontier: set[TableName],
        remaining: int,
    ) -> list[Row]:
        """Collect only edges leaving the current reachable table frontier."""
        sources = self._spark.createDataFrame(
            [(table.catalog, table.schema_name, table.table) for table in frontier],
            schema=", ".join(f"{name} STRING" for name in self._SOURCE_COLUMNS),
        )

        # The extra row detects budget overflow without unbounded collection.
        return (
            dependencies.join(
                F.broadcast(sources),
                on=list(self._SOURCE_COLUMNS),
                how="left_semi",
            )
            .distinct()
            .limit(remaining + 1)
            .collect()
        )

    def _resolve_inherited_rules(
        self,
        explicit_rules: RetentionRules,
        lineage: _TableLineage,
        views: set[TableName],
    ) -> RetentionRules:
        """Propagate one effective policy per node; never emit VIEW rules."""
        explicit_by_table = {rule.table_name: rule for rule in explicit_rules}
        incoming: dict[TableName, dict[tuple[str, int], RetentionRule]] = defaultdict(
            dict
        )
        inherited: list[RetentionRule] = []

        for table in self._order_downstream_tables(lineage):
            candidates = incoming.pop(table, {})
            policy = explicit_by_table.get(table)

            if policy is None:
                if len(candidates) != 1:
                    continue
                policy = next(iter(candidates.values()))
                if table not in views:
                    inherited.append(policy)

            for downstream in lineage.get(table, ()):
                candidate = RetentionRule(
                    table_name=downstream,
                    time_column=policy.time_column,
                    expiration_days=policy.expiration_days,
                )
                identity = (candidate.time_column.lower(), candidate.expiration_days)
                existing = incoming[downstream].get(identity)
                # Equal policies should produce the same snapshot in either order.
                if existing is None or candidate.time_column < existing.time_column:
                    incoming[downstream][identity] = candidate

        return tuple(sorted(inherited, key=lambda rule: rule.table_name.sql_identifier))

    @staticmethod
    def _order_downstream_tables(lineage: _TableLineage) -> Iterator[TableName]:
        """Visit each downstream node after its inputs, relying on SDP's DAG."""
        order = TopologicalSorter()

        for source, targets in lineage.items():
            for target in targets:
                order.add(target, source)

        return order.static_order()
