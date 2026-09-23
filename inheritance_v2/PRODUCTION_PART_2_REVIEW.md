# Retention inheritance: review findings and production Part 2 plan

Reviewed 2026-09-23 against this repository's isolated `inheritance_v2/` POC,
the current packaged baseline, and Azure Databricks documentation. The POC
was originally introduced at `81b04e8`.
This is a handover to the agent working on the **separate production PR**. We
cannot inspect that PR or the Databricks workspace from this checkout. Treat
every description of production state below as a task to verify there, not an
assertion that the production branch matches this repository.

## Decision for the two PRs

| Stage | Expected behavior | Release decision |
| --- | --- | --- |
| Part 1, currently in the production PR | Generate, validate, and publish **explicit** rules. Inheritance is off. | Verify every production entry point keeps inheritance off. Do not accidentally expose the old column-lineage resolver. |
| Part 2, separate PR | Add optional table-lineage inheritance and validate the **effective** rules before publication. | Only enable it after the live VIEW, policy semantics, update coverage, and snapshot gates below pass. |

The repo's `src/ttl_config/retention_config_generator.py` defaults
`include_inheritance=False`, lazily imports the **old column-lineage resolver**
when set to `True`, calls `generate_ttl_config(...)`, and writes through
`write_ttl_config(...)`. `inheritance_v2/` is not imported by the generator or
included in the package under `src/`. This checkout does **not** implement the
environment-resolution change described in the historical
`archive/agent-prompts/add-environment-catalog-resolution.md`; the production
agent must use its real branch as the source of truth. In that branch, physical
environment catalogs must be resolved **before** lineage traversal.

## Review findings

The labels distinguish demonstrated repository behavior, documented platform
behavior, and outstanding workspace/business evidence. There is no demonstrated
policy-algorithm defect in the POC under its stated contracts. Passing local
tests is insufficient evidence for those contracts in production.

### R1 — VIEW traversal is an unverified launch blocker

**Evidence:** `inheritance_v2/retention_rule_inheritance.py` admits VIEW nodes
only through `system.access.table_lineage` rows whose `direct_access` is true,
both types are in TABLE/STREAMING_TABLE/VIEW, and whose pipeline/update IDs
match the latest successful update. Synthetic tests fabricate both
`STREAMING_TABLE -> VIEW` and `VIEW -> STREAMING_TABLE` edges. Databricks
documents that lineage tables capture only a subset of read/write events, and
that `direct_access=false` may reflect view expansion rather than a direct
edge. Its lineage limitations include incomplete coverage of some pipeline
PRIVATE tables. None of those documents proves that both edges appear for our
real pipeline.

**Part 2 action:** Run the read-only pre-flight below using the **production
job's service principal** on a known representative pipeline. Record both
direct edges, types, identity, and update metadata for the same relevant
completed update. If either edge is absent, has `direct_access=false`, or
cannot be joined to the selected update, **stop Part 2** and show the rows or
exact access error. Do not infer a connection from read-only/write-only rows,
`event_id`, position, column lineage, or query plans.

The pipeline timeline is documented as Public Preview; confirm it is available
and accessible under the production job identity in the relevant region.

### R2 — Matching column names are a business invariant, not proof of meaning

**Evidence:** The POC copies `source_rule.time_column` and
`source_rule.expiration_days` unchanged. The current
`RetentionTargetValidator` checks whether that name exists in the physical
table and whether its type is DATE/TIMESTAMP/TIMESTAMP_NTZ. A downstream
transformation could still produce an unrelated timestamp with that name.
Neither schema validation nor table-level lineage proves the column represents
the same event time.

**Part 2 action:** Confirm with owners of the supported Bronze/Silver flows that
the configured retention timestamp keeps **both its name and intended time
meaning** on paths that should inherit. If this is not guaranteed for a path,
configure its target explicitly. Do not add column-mapping guesses or move
physical validation into the resolver. A renamed/missing field will fail
target validation, as intended; a same-named semantic change will not be
detected by this feature.

### R3 — Latest successful pipeline update can be a selective refresh

**Evidence:** `_read_latest_completed_updates()` takes one completed REFRESH or
FULL_REFRESH per `(workspace_id, pipeline_id)` by `period_end_time` before
joining lineage. This correctly prevents an **older removed edge** from
leaking in. Azure Databricks also permits updates of selected tables and
refreshes of failed tables. The timeline exposes `refresh_selection` and
`full_refresh_selection`. A selected update may not emit edges for every
unchanged table. Because lineage is event data, not a graph snapshot, this
algorithm can then lose valid derived rules temporarily. This is a
**false-negative/availability consequence**, not proof of a stale-edge bug.

**Part 2 action:** Inspect actual update cadence and selection fields. If the
retention job runs after complete, representative updates and occasional
missing derived rules are acceptable, retain the simple latest-successful
update filter. If selective refresh is common and the complete authoritative
snapshot must remain stable, **pause enabling inheritance** and agree on a
business-level scheduling/configuration decision. Do not recover edges from
older events: that can resurrect deleted dependencies and create false-positive
retention. Also account for lineage ingestion delay and the system tables'
rolling one-year retention.

### R4 — Explicit-only fallback can shrink an authoritative snapshot

**Evidence:** `_read_lineage()` catches `PySparkException` and returns explicit
rules when metadata is unavailable; budget overflow does likewise. The
generator writes all supplied rules as an authoritative overwrite. Thus a
transient lineage failure after a previous successful run may omit previously
inherited entries. The generator's own docstring assigns removal of active
target policies to the downstream TTL applicator. Neither the local POC nor
this checkout proves that production consumer behavior is acceptable.

**Part 1 / Part 2 action:** In Part 1, verify no derived rows already exist
before the first explicit-only authoritative overwrite; if they do, agree on
the intended policy removal. For Part 2, decide whether loss of derived rows
on metadata failure is an acceptable optional-enrichment contract, including
what the downstream applicator does with removed configuration rows. Test one
prior snapshot followed by explicit-only fallback at the generator/writer
boundary. If removal is unacceptable, **do not turn inheritance on** until the
business publication contract is agreed. Do not silently add historical
reconstruction, cached policies, or a second reconciliation pass.

### R5 — Production branch divergence must be measured, not guessed

**Evidence:** This checkout's generator has neither a required environment
argument nor the environment-resolution implementation described in a later
agent prompt. Its `tests/` directory contains only domain-model tests, while
the POC's Spark tests are under `inheritance_v2/tests/`. The production PR may
already have different interfaces, tests, flag handling, and validator logic.

**Part 2 action:** Before editing production, compare its HEAD and Part 1 diff
with this checkout. Record the real package names, method signatures, catalog
resolution order, validator behavior, output destination, flag defaults, job
invocations, and tests. Integrate the **algorithm**, not the POC file or its
`ttl_config` imports. Preserve established production improvements. Do not
reimplement environment mapping inside inheritance.

### R6 — Documentation in this checkout named the wrong API and destination

**Evidence:** Before this review, root `README.md` called nonexistent
`generate_retention_config(...)` / `write_retention_config(...)`, opted into
column inheritance in the example, and named the destination
`<ops_catalog>.retention.retention_config`. `docs/retention-design.md` also
named that destination. The generator actually exposes `generate_ttl_config`
and `write_ttl_config`; `_RETENTION_TABLE` is `ttl_config`.

**Resolution:** Corrected those two documents in the previous docs commit and
made the default-disabled, isolated-POC status explicit. The production agent
should update **its** documentation against **its** generator, not copy this
example.

### R7 — Keep destination partition validation; do not apply it to targets

**Evidence:** `RetentionConfigGenerator.write_ttl_config()` writes a complete
rules snapshot to a managed Delta config table and checks an existing
destination's format and `partitionColumns`. A partitioned Delta table can
retain untouched partitions under
[dynamic partition overwrite](https://learn.microsoft.com/en-us/azure/databricks/delta/selective-overwrite),
including DataFrame overwrite writes, while the writer does not explicitly
pin an overwrite mode. Omitted policies must not survive a snapshot rewrite.
The `RetentionTargetValidator` checks managed Delta/Iceberg or Delta streaming
targets and top-level time columns; it does **not** restrict target partitions.

**Part 1 / Part 2 action:** Keep the existing unpartitioned check on the
**configuration destination**. Do not add partition validation to physical
retention targets. If the production writer uses a demonstrably different
full-snapshot operation, reassess the guard against that real writer rather
than deleting it as a generic cleanup.

### Code-quality review: keep the POC's shallow method hierarchy

The old resolver in this checkout is 396 lines, with 13 methods and two
lineage type aliases. The isolated table resolver is 282 lines, with 10 methods
and one lineage alias. Those are **POC baselines**, not production PR metrics.
The new public method reads as acquisition, policy resolution, then return of
effective rules. `_read_current_dependencies` and its two reads own Spark
filtering and update choice; `_collect_reachable_lineage` and
`_read_frontier_dependencies` separate graph traversal from a bounded Spark
action; `_resolve_inherited_rules` and `_order_downstream_tables` own one
coherent DAG propagation. Keep these chapters as they stand unless a specific
production defect calls for change. Splitting the policy loop or adding node,
edge, or policy classes would make the code harder to follow. The test suite
verifies synthetic Spark and policy decisions; it does not prove live VIEW
capture, semantic column preservation, or snapshot-consumer behavior.

## Read-only workspace pre-flight for Part 2

Substitute real identifiers for the five placeholders. Query as the job's
run-as identity. Use a production-like update that should expose both VIEW
edges. First identify a known `workspace_id`, `pipeline_id`, and physical
source/view/target names; then inspect rows with this query:

```sql
WITH latest_completed AS (
    SELECT
        workspace_id,
        pipeline_id,
        update_id,
        update_type,
        period_end_time,
        refresh_selection,
        full_refresh_selection,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, pipeline_id
            ORDER BY period_end_time DESC
        ) AS update_rank
    FROM system.lakeflow.pipeline_update_timeline
    WHERE workspace_id = '<workspace-id>'
      AND pipeline_id = '<pipeline-id>'
      AND result_state = 'COMPLETED'
      AND update_type IN ('REFRESH', 'FULL_REFRESH')
)
SELECT
    l.workspace_id,
    u.pipeline_id,
    u.update_id,
    u.update_type,
    u.period_end_time,
    u.refresh_selection,
    u.full_refresh_selection,
    l.event_time,
    l.event_id,
    l.direct_access,
    l.source_table_catalog,
    l.source_table_schema,
    l.source_table_name,
    l.source_table_full_name,
    l.source_type,
    l.target_table_catalog,
    l.target_table_schema,
    l.target_table_name,
    l.target_table_full_name,
    l.target_type
FROM system.access.table_lineage AS l
JOIN latest_completed AS u
  ON l.workspace_id = u.workspace_id
 AND l.entity_metadata.dlt_pipeline_info.dlt_pipeline_id = u.pipeline_id
 AND l.entity_metadata.dlt_pipeline_info.dlt_update_id = u.update_id
WHERE u.update_rank = 1
  AND l.metastore_id = substring_index(current_metastore(), ':', -1)
  AND (
      l.source_table_full_name IN (
          '<bronze-or-silver-upstream>', '<bronze-or-silver-view>'
      )
      OR l.target_table_full_name IN (
          '<bronze-or-silver-view>', '<bronze-or-silver-downstream>'
      )
  )
ORDER BY l.event_time DESC, l.event_id;
```

Compare the component catalog/schema/table fields when debugging name or case
mismatches; the POC matches those fields, not `*_full_name`. Check
`source_type`, `target_type`, and `direct_access` for **each** bridge edge.
`event_id` identifies an event; it is not a source/target join key. If the
query yields no bridge, inspect those three named objects in raw recent
`table_lineage` and the matching timeline rows before reporting a failure.
Do not use an older update's edges to complete a current bridge. Run the
same check for ordinary streaming-table edges, the production metastore, and
a newer failed update. Save redacted output or a precise query/result summary
for Part 2 review; this checkout has no live workspace evidence.

## Exact production Part 2 work sequence

1. **Freeze Part 1 behavior.** Verify inheritance is off in all jobs, CLI
   entry points, and unit/integration tests. Confirm whether the production
   public flag still allows old inheritance; do not opt in before replacement.
   Compare the published snapshot with explicit rules and check the downstream
   removal contract if inherited rows have ever been published.
2. **Read the real branch.** Apply the repository review and anti-overengineering
   skills. Inspect current resolver, Excel reader, domain models, environment
   catalog resolution, generator, target validator, tests, job parameters,
   write contract, and documentation. List exact divergences from this POC.
   The deleted `ttl_config_v2/` duplicate survives only in Git history;
   historical prompts live under `archive/agent-prompts/`. Use the active
   handover and production branch, not those old files, as instructions.
3. **Prove the gates.** Capture both VIEW bridge edges in the latest successful
   relevant update under production permissions. Confirm same-name **semantic**
   invariant, supported table/view types, update coverage, normal refresh
   pattern, and accepted behavior when derived rows disappear.
4. **Replace old inheritance only.** In the production resolver, use direct
   `system.access.table_lineage` filtered to the current metastore; allow only
   TABLE/STREAMING_TABLE/VIEW on both ends and `direct_access=true`; require
   `bronze` or `silver` in both catalog names, ignoring case. Retain only the
   latest completed REFRESH/FULL_REFRESH per workspace/pipeline, joined on
   update ID. Use a reachable table-level frontier and one driver row budget.
   Exclude PATH and MATERIALIZED_VIEW naturally by supported types.
5. **Propagate simple policies.** Model a graph of `TableName -> set[TableName]`
   plus a small VIEW set. Traverse in Lakeflow DAG order. Explicit rules win;
   equal `(time_column.lower(), expiration_days)` candidates collapse
   deterministically; conflicting candidates stop at that node, while an
   independent valid path can reach a later node. Pass through VIEW but never
   emit its rule. Copy time-column **name** and expiration unchanged; no
   target-column lookup in inheritance. Keep Spark lineage failures and budget
   overflow as explicit-only fallback.
6. **Delete obsolete code and tests.** Remove `_TimeColumn`,
   `_TimeColumnLineage`, column-specific frontier/mapping parsing,
   `source_time_column`, `target_time_column`, inferred column rename and
   target-column trimming safeguards, nested-column filtering, and tests
   solely defending these behaviors. Do not retain a column-lineage fallback.
7. **Preserve ownership and add focused tests.** Environment resolution
   precedes inheritance. Validation of the **effective** rules follows it;
   `RetentionTargetValidator` rejects missing or wrong-type downstream time
   columns. Unit tests cover transitive propagation, explicit overrides,
   equal/conflicting policies, intermediate ambiguity, independent valid
   paths, VIEW omission, Spark fallback, and budget behavior. Synthetic Spark
   tests cover the system-table filters, latest update, removed old edge,
   case-insensitive catalogs, MV/PATH exclusion, and reachable frontier. Add
   one generator/validator boundary test for a missing downstream column and
   one snapshot/fallback test for the agreed authoritative-write behavior.
8. **Verify and review.** Run focused then broad tests and normal static checks
   on the real branch. Check method order against call order, bounded driver
   collection and Spark actions, snapshot consistency, flag defaults, and
   doc/API accuracy. Measure resolver file LOC, methods, aliases, and major
   state before/after. If not materially simpler than old code, simplify
   before committing. Fix only demonstrated problems; do not create a graph
   framework, cycle recovery, historical reconstruction, schema reads inside
   inheritance, SQLSTATE taxonomy, or extra budgets.
9. **Release deliberately.** Leave the default disabled unless enabling
   inheritance was an explicit Part 2 release decision. Confirm deployment
   configuration and inspect effective rules in a representative environment
   before the authoritative write. Give one verdict and a production commit
   SHA only for changes actually made in that production repository.

The detailed implementation contract and focused test checklist remain in
[`PORT_TO_PRODUCTION.md`](PORT_TO_PRODUCTION.md). Its suggested module names
are examples, not overrides of the production branch.

## Official platform references checked 2026-09-23

- [Azure Databricks lineage system tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/lineage): table-lineage fields, direct access, best-effort capture, entity metadata, rolling retention.
- [Azure Databricks pipeline timeline](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/jobs): update IDs, result/type fields, selection fields, completed rows.
- [Azure Databricks pipeline updates](https://learn.microsoft.com/en-us/azure/databricks/ldp/updates): selected-table and failed-table refresh behavior.
- [Azure Databricks lineage limitations](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage): lineage visibility and PRIVATE-table limitations.
- [Azure Databricks current_metastore()](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/current_metastore): `<cloud>:<region>:<uuid>` return format.
- [Azure Databricks selective Delta overwrite](https://learn.microsoft.com/en-us/azure/databricks/delta/selective-overwrite): dynamic partition overwrite can leave untouched partitions in an existing table.
