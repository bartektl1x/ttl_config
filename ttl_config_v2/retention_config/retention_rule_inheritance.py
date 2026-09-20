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

from retention_config.retention_rules import RetentionRule, RetentionRules, TableName

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

type _TimeColumn = tuple[TableName, str]
type _TimeColumnLineage = dict[_TimeColumn, set[_TimeColumn]]

_LOGGER = logging.getLogger(__name__)


class RetentionRuleInheritanceResolver:
    """Inherit retention rules along SDP's acyclic table dependencies.

    Explicit rules win. Only unambiguous policies propagate, using mappings from
    each pipeline's latest successful refresh. Missing mappings have no historical
    fallback; later failed updates are ignored. SDP owns topology and ownership.
    """

    _SOURCE_COLUMNS = (
        "source_catalog",
        "source_schema",
        "source_table",
        "source_time_column",
    )
    _UPDATE_COLUMNS = ("workspace_id", "pipeline_id", "update_id")

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
        """Add downstream policies supported by SDP's successful-update lineage."""
        lineage = self._read_lineage(explicit_rules)

        if not lineage:
            return explicit_rules

        inherited_rules = self._resolve_inherited_rules(
            explicit_rules,
            lineage,
        )

        return self._combine_retention_rules(
            explicit_rules,
            inherited_rules,
        )

    def _read_lineage(
        self,
        explicit_rules: RetentionRules,
    ) -> _TimeColumnLineage:
        """Skip optional enrichment if Spark cannot obtain the lineage mappings."""
        try:
            current_mappings = self._read_current_mappings()

            return self._collect_reachable_lineage(
                explicit_rules,
                current_mappings,
            )
        except PySparkException:
            _LOGGER.warning(
                "Retention lineage read failed; keeping explicit rules",
                exc_info=True,
            )
            return {}

    def _read_current_mappings(self) -> DataFrame:
        """Select direct mappings from each pipeline's latest successful refresh."""
        direct_mappings = self._read_direct_column_mappings()
        latest_updates = self._read_latest_completed_updates()

        return (
            direct_mappings
            .join(
                latest_updates,
                on=list(self._UPDATE_COLUMNS),
                how="left_semi",
            )
            .select(
                *self._SOURCE_COLUMNS,
                "target_catalog",
                "target_schema",
                "target_table",
                "target_time_column",
            )
        )

    def _read_direct_column_mappings(self) -> DataFrame:
        """Read direct top-level table-to-table column mappings."""
        return (
            self._spark.table("system.access.column_lineage")
            # column_lineage stores only the UUID, while current_metastore()
            # returns <cloud>:<region>:<uuid>.
            .where(
                "metastore_id = substring_index(current_metastore(), ':', -1)"
            )
            .where(
                (F.col("direct_access") == F.lit(True))
                & F.col("source_type").isin("TABLE", "STREAMING_TABLE")
                & F.col("target_type").isin("TABLE", "STREAMING_TABLE")
                & (F.length(F.trim("target_column_name")) > 0)
                & (F.instr("target_column_name", ".") == 0)
            )
            .select(
                F.lower("source_table_catalog").alias("source_catalog"),
                F.lower("source_table_schema").alias("source_schema"),
                F.lower("source_table_name").alias("source_table"),
                F.lower("source_column_name").alias("source_time_column"),
                F.lower("target_table_catalog").alias("target_catalog"),
                F.lower("target_table_schema").alias("target_schema"),
                F.lower("target_table_name").alias("target_table"),
                F.col("target_column_name").alias("target_time_column"),
                "workspace_id",
                F.col(
                    "entity_metadata.dlt_pipeline_info.dlt_pipeline_id"
                ).alias("pipeline_id"),
                F.col(
                    "entity_metadata.dlt_pipeline_info.dlt_update_id"
                ).alias("update_id"),
            )
        )

    def _read_latest_completed_updates(self) -> DataFrame:
        """Select the latest completed refresh for each workspace/pipeline."""
        latest_first = Window.partitionBy(
            "workspace_id",
            "pipeline_id",
        ).orderBy(F.desc("period_end_time"))

        # Select the latest completed update before joining mappings, so absent
        # mappings cannot expose an older update.
        return (
            self._spark.table("system.lakeflow.pipeline_update_timeline")
            .where(
                (F.col("result_state") == "COMPLETED")
                & F.col("update_type").isin("REFRESH", "FULL_REFRESH")
            )
            .select(
                *self._UPDATE_COLUMNS,
                "period_end_time",
            )
            .withColumn(
                "update_rank",
                F.row_number().over(latest_first),
            )
            .where(F.col("update_rank") == 1)
            .select(*self._UPDATE_COLUMNS)
        )

    def _collect_reachable_lineage(
        self,
        explicit_rules: RetentionRules,
        current_mappings: DataFrame,
    ) -> _TimeColumnLineage:
        """Collect reachable time-column mappings within one driver-memory budget."""
        explicit_tables = {rule.table_name for rule in explicit_rules}
        frontier = {
            (rule.table_name, rule.time_column.lower())
            for rule in explicit_rules
        }

        queried_columns: set[_TimeColumn] = set()
        lineage: _TimeColumnLineage = defaultdict(set)
        collected_mapping_count = 0

        while frontier:
            queried_columns.update(frontier)
            remaining_mappings = self._max_mappings - collected_mapping_count

            mappings = self._read_frontier_mappings(
                current_mappings,
                frontier,
                remaining_mappings,
            )

            if len(mappings) > remaining_mappings:
                _LOGGER.warning(
                    "Retention lineage exceeds max_mappings=%s; skipping inheritance",
                    self._max_mappings,
                )
                return {}

            collected_mapping_count += len(mappings)
            frontier = set()

            for source, target in self._parse_column_mappings(mappings):
                target_table, target_time_column = target

                if target_table in explicit_tables:
                    continue

                lineage[source].add(target)

                downstream_source = (target_table, target_time_column.lower())

                if downstream_source not in queried_columns:
                    frontier.add(downstream_source)

        return lineage

    def _read_frontier_mappings(
        self,
        current_mappings: DataFrame,
        frontier: set[_TimeColumn],
        remaining_mappings: int,
    ) -> list[Row]:
        """Read and collect mappings for the current lineage frontier."""
        sources = self._spark.createDataFrame(
            [
                (
                    table.catalog,
                    table.schema_name,
                    table.table,
                    time_column,
                )
                for table, time_column in frontier
            ],
            schema=", ".join(
                f"{name} STRING"
                for name in self._SOURCE_COLUMNS
            ),
        )

        # Collect one extra row to detect budget overflow.
        return (
            current_mappings
            .join(
                F.broadcast(sources),
                on=list(self._SOURCE_COLUMNS),
                how="left_semi",
            )
            .distinct()
            .limit(remaining_mappings + 1)
            .collect()
        )

    @staticmethod
    def _parse_column_mappings(
        mappings: list[Row],
    ) -> Iterator[tuple[_TimeColumn, _TimeColumn]]:
        """Decode column mappings, skipping malformed table identities."""
        for mapping in mappings:
            try:
                source_table = TableName(
                    catalog=mapping["source_catalog"],
                    schema=mapping["source_schema"],
                    table=mapping["source_table"],
                )
                target_table = TableName(
                    catalog=mapping["target_catalog"],
                    schema=mapping["target_schema"],
                    table=mapping["target_table"],
                )
            except ValidationError:
                continue

            yield (
                (
                    source_table,
                    mapping["source_time_column"],
                ),
                (
                    target_table,
                    mapping["target_time_column"],
                ),
            )

    def _resolve_inherited_rules(
        self,
        explicit_rules: RetentionRules,
        lineage: _TimeColumnLineage,
    ) -> RetentionRules:
        """Resolve each table after its upstream inputs, propagating accepted rules."""
        explicit_by_table = {
            rule.table_name: rule
            for rule in explicit_rules
        }

        incoming_candidates: dict[
            TableName,
            dict[tuple[str, int], RetentionRule],
        ] = defaultdict(dict)

        inherited_rules: list[RetentionRule] = []

        for table in self._order_downstream_tables(lineage):
            candidates = incoming_candidates.pop(table, {})
            source_rule = explicit_by_table.get(table)

            if source_rule is None:
                if len(candidates) != 1:
                    continue

                source_rule = next(iter(candidates.values()))
                inherited_rules.append(source_rule)

            source = (table, source_rule.time_column.lower())

            for downstream_table, time_column in lineage.get(source, ()):
                candidate = self._inherit_rule(
                    source_rule,
                    downstream_table,
                    time_column,
                )

                if candidate is None:
                    continue

                policy = (
                    candidate.time_column.lower(),
                    candidate.expiration_days,
                )

                incoming_candidates[downstream_table].setdefault(
                    policy,
                    candidate,
                )

        return tuple(inherited_rules)

    @staticmethod
    def _order_downstream_tables(
        lineage: _TimeColumnLineage,
    ) -> Iterator[TableName]:
        """Put each table after its upstream inputs, relying on SDP's DAG contract."""
        dependency_order = TopologicalSorter()

        for (source_table, _), downstream_columns in lineage.items():
            for downstream_table, _ in downstream_columns:
                dependency_order.add(
                    downstream_table,
                    source_table,
                )

        return dependency_order.static_order()

    @staticmethod
    def _inherit_rule(
        source_rule: RetentionRule,
        target_table: TableName,
        target_time_column: str,
    ) -> RetentionRule | None:
        """Copy the policy to an observed column only if the inferred rule is valid."""
        try:
            candidate = RetentionRule(
                table_name=target_table,
                time_column=target_time_column,
                expiration_days=source_rule.expiration_days,
            )
        except ValidationError:
            return None

        # Pydantic trimming must not silently select another physical column.
        if candidate.time_column != target_time_column:
            return None

        return candidate

    @staticmethod
    def _combine_retention_rules(
        explicit_rules: RetentionRules,
        inherited_rules: RetentionRules,
    ) -> RetentionRules:
        """Combine authoritative explicit rules with derived rules."""
        return (
            *explicit_rules,
            *sorted(
                inherited_rules,
                key=lambda rule: rule.table_name.sql_identifier,
            ),
        )
