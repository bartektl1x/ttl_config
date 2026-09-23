# Production handover prompt: port table-lineage inheritance v2

**Start with [`PRODUCTION_PART_2_REVIEW.md`](PRODUCTION_PART_2_REVIEW.md).**
Its verified findings and deployment gates supplement this implementation
prompt. Part 1 already uses explicit rules with inheritance disabled; do not
change its production behavior until Part 2 passes those gates. This repository
is a design POC, not a checkout of the production PR. Inspect production HEAD
and its diff against Part 1 before making any change.

You are taking the isolated POC in `inheritance_v2/` into **our actual
production retention repository**. Act as a Principal Data Platform Engineer.
Deliver a small, reviewed implementation on the production branch, with
focused tests, verification, and a commit. Follow the production repository's
own engineering skills, conventions, and interfaces before using this POC.

## Sources and priority

1. The **current checked-out production branch** is authoritative. Inspect its
   actual resolver, domain models, Excel reader, generator, validator, callers,
   tests, configuration, and local repository instructions. Do not assume its
   paths, package names, constructor, or environment catalog resolution match
   this POC.
2. Read this repository's `skills/repo-principal-review/SKILL.md` and
   `skills/ai-engineering-anti-overengineering/SKILL.md` for the intended
   engineering philosophy. Follow any corresponding instructions in the
   production repository.
3. Read `inheritance_v2/retention_rule_inheritance.py`, both test files in
   `inheritance_v2/tests/`, and `inheritance_v2/README.md`. They demonstrate
   the chosen algorithm and its synthetic verification. They are not evidence
   that live VIEW lineage works in your workspace.
4. Use `docs/agent-prompts/redesign-retention-inheritance-table-lineage.md`
   and this handover for business intent. Resolve differences against the real
   production code and actual Databricks observations. Do not overwrite
   production improvements by copying this file wholesale.
5. Check the production generator's default and every invocation. An old
   column-lineage resolver must not be reachable in production while Part 1
   deliberately disables inheritance. Part 2 should replace that resolver
   before any caller opts in; keep the default disabled unless the product
   explicitly decides otherwise.

## Mandatory live pre-flight: VIEW bridge

Before editing production code, inspect a recent successful Lakeflow update
in the **actual workspace** with a known production-like chain:

```text
upstream STREAMING_TABLE -> VIEW -> downstream STREAMING_TABLE
```

Query `system.access.table_lineage`, inspecting both edges for the same
relevant successful pipeline update. Collect source and target catalog,
schema, name, type, `direct_access`, `workspace_id`, pipeline ID, update ID,
`event_time`, and `event_id`. Verify that the upstream table-to-view edge and
the view-to-downstream-table edge are present as **direct** relationships,
with types sufficient for this graph. Check
`system.lakeflow.pipeline_update_timeline` to confirm the update is a
completed REFRESH or FULL_REFRESH. Do not infer an edge from a `direct_access =
false` view expansion or from unrelated read/write events.

If the two edges cannot be verified reliably, **stop before production
changes**. Show the observed rows or exact access/error evidence and do not
invent a fallback. The POC's local Spark tests do not satisfy this gate.
Run this check as the production job's service principal. Verify whether
partial/selective updates are used and whether a temporary lineage failure may
remove previously inherited rows from the authoritative snapshot. See the
read-only SQL and decisions in `PRODUCTION_PART_2_REVIEW.md`.

## What changed in the isolated POC

The old `src/ttl_config/retention_rule_inheritance.py` remains untouched.
`inheritance_v2/retention_rule_inheritance.py` is an alternative resolver;
the generator does not import it. Its policy algorithm and system-table query
changed as follows:

| Previous column-lineage POC | New isolated table-lineage POC |
| --- | --- |
| `system.access.column_lineage` | `system.access.table_lineage` |
| `_TimeColumn = (TableName, time_column)` | TableName alone is the graph node |
| `_TimeColumnLineage` | `_TableLineage = dict[TableName, set[TableName]]` |
| `source_time_column` / `target_time_column` lineage fields | No column fields in lineage or frontier |
| `_read_direct_column_mappings` | `_read_direct_table_dependencies` |
| `_read_current_mappings` | `_read_current_dependencies` |
| `_read_frontier_mappings` keyed by table and column | `_read_frontier_dependencies` keyed by table only |
| `_parse_column_mappings` | TableName conversion in the reachable traversal |
| `_inherit_rule` with inferred target-column validation and trimming workaround | Direct `RetentionRule` with unchanged source column and expiration |
| TABLE / STREAMING_TABLE only | TABLE / STREAMING_TABLE as targets; VIEW as traversal only |
| No Bronze/Silver catalog gate | Both endpoint catalogs must contain `bronze` or `silver`, ignoring case |
| `_combine_retention_rules` | Deterministic sort at the end of policy resolution |

Keep from the old design: explicit rules are authoritative; equal policies
collapse; ambiguous nodes do not propagate; an independent valid path can
still reach a downstream node; lineage failure returns explicit rules; the
latest completed relevant update is selected per `(workspace_id, pipeline_id)`;
and reachable Spark collections have one `max_mappings` safety budget.

## Exact policy and responsibility contract

- An explicit `event_time / 30` rule propagates as `event_time / 30`. Do not
  infer `event_time -> created_at` or use column lineage as a fallback.
- The **table lineage** decides where the policy can travel. A view is an
  internal traversal node: carry the policy through it but never return a
  `RetentionRule` for it. No VIEW rule reaches the physical target validator.
- TABLE and STREAMING_TABLE can receive inherited rules. Restrict both
  source and target types to `TABLE`, `STREAMING_TABLE`, `VIEW`. This naturally
  excludes MATERIALIZED_VIEW and PATH, including traversal beyond them.
- Restrict **both** source and target catalogs with case-insensitive substring
  checks for `bronze` or `silver`. Do not hardcode `dev_`, `qat_`, `uat_`,
  `prp_`, or `prd_`. The resolver receives physical catalog names after any
  production environment-resolution step.
- The Excel reader owns workbook validation; Pydantic models own local
  invariants; the inheritance resolver owns topology and propagation; the
  existing `RetentionTargetValidator` owns table existence/type and physical
  time-column checks; the generator owns orchestration and output. Ensure
  validation still happens **after** optional inheritance.
- An inherited rule for a downstream table missing that same time-column
  name must fail in `RetentionTargetValidator`. Do not add a schema lookup or
  downstream-column guessing to the resolver.
- SDP topology is a DAG; ownership is stable; latest successful update is
  sufficient. Missing lineage means no derived rule. False-negative
  inheritance is preferable to false-positive retention. No cycle recovery,
  historical reconciliation, generic graph framework, or dual lineage path.
- Retaining the same name and physical type does not prove the downstream
  column has the same time semantics. Confirm that invariant for the actual
  supported transformations. If it is not guaranteed, leave that target
  explicit; do not add mapping guesses to the resolver.

## Algorithm to adapt to the production branch

1. **Read candidate edges.** Filter `system.access.table_lineage` to the
   current metastore, `direct_access = true`, the three allowed types on
   **both** endpoints, and Bronze/Silver membership on **both** catalogs.
   Normalize table identity case; select source and target table components,
   target type, workspace ID, and pipeline/update IDs. Do not select columns
   or carry unused event metadata into graph traversal.
2. **Remove historical edges.** Select the latest completed REFRESH or
   FULL_REFRESH in `system.lakeflow.pipeline_update_timeline` per workspace
   and pipeline **before** joining the candidate edges. A newer failed update
   is ignored; a newer successful update with no mapping must not expose an
   older mapping. Semi-join by workspace, pipeline, and update ID. Preserve
   any better production implementation of this protection.
3. **Collect only reachable edges.** Seed a frontier with the explicit rule
   tables. Broadcast the current table-identity frontier against the filtered
   dependencies. Deduplicate, then `limit(remaining_budget + 1).collect()`.
   Discard all inheritance on overflow. Add edges to a plain
   `dict[TableName, set[TableName]]`; record VIEW targets in one small set.
   Advance with as-yet-unqueried target tables. Do not load all account
   lineage into Python, add caches, or introduce extra budgets.
4. **Resolve policies in DAG order.** The explicit rule at a node takes
   precedence. Otherwise, accept exactly one unique incoming policy,
   identified by `(time_column.lower(), expiration_days)`; no candidate or
   conflicting candidates mean no propagation from that node. Build each
   downstream candidate with the **same** time-column string and expiration.
   When equal policies arrive with different column-name casing, choose a
   deterministic source spelling so an authoritative snapshot cannot change
   merely because lineage rows arrive in another order.
   Append accepted rules for TABLE / STREAMING_TABLE only; let VIEW nodes
   continue propagation internally. An ambiguous intermediate stops, but
   another valid upstream path into a later node remains eligible.
5. **Return effective rules.** Keep explicit rules and append inherited rules
   in deterministic table-name order. Catch relevant `PySparkException`
   failures across lazy lineage construction **and actions**, log, and return
   explicit rules only. Keep the existing public constructor parameter
   `max_mappings` if the production callers use it; terminology inside the
   implementation may call them dependencies.

## Focused production tests

Adapt existing tests rather than copying the POC's test files mechanically.
Delete tests that exist only for target-column rename inference, column
mapping parsing, Pydantic target-column trimming, or column-specific frontier
matching. Verify each distinct behavior:

- A -> B -> C transitive inheritance with unchanged column and days.
- Explicit B override of A, with B's policy reaching C.
- Equal incoming policies collapse (including case-insensitive column
  identity); conflicting expiration or column names stop at the target.
- Ambiguous intermediate does not propagate; a separate valid path still
  reaches a downstream target.
- `STREAMING_TABLE -> VIEW -> STREAMING_TABLE` propagates through the view
  without emitting a VIEW rule.
- Spark lineage read/action failure returns explicit rules; budget overflow
  returns explicit rules; invalid public budget input is rejected.
- Spark/system-table fixtures use `system.access.table_lineage` fields, with
  **no** fake source/target column fields. Verify latest successful update,
  newer failed update, removed older mapping, other metastore,
  `direct_access=false`, TABLE / STREAMING_TABLE / VIEW support, PATH / MV
  exclusion, Bronze/Silver on both sides, mixed-case catalog names, and
  bounded reachable traversal.
- Preserve or add a generator/validator integration test showing that a
  propagated rule with a missing downstream physical time column is rejected
  **by the target validator**, if the production suite lacks that boundary
  check. Do not duplicate physical validation inside inheritance.

The isolated POC's integration tests use synthetic Spark DataFrames and a
test-only `current_metastore()` UDF to run locally. They do not inspect a
Databricks workspace or prove its VIEW-event behavior.

## Delivery and review

Before editing, report the **actual production** differences from this POC
and name the specific obsolete symbols you plan to remove. Keep unrelated
environment resolution, Excel parsing, validator logic, packaging, and
output behavior untouched. Avoid new managers, services, graph classes,
configuration patterns, or small helpers that merely relocate obvious code.

Run focused unit and Spark tests, then broader retention checks and the
production repository's normal static checks. Inspect the whole diff with
fresh eyes: Spark action bounds, stale-update leakage, explicit overrides,
view omission, catalog gates, ownership of physical validation, unnecessary
states, and whether the final resolver reads top-down. Fix concrete issues,
not stylistic preferences. Report production resolver LOC, methods, aliases,
and conceptual state before/after. The new resolver should be materially
shorter; if it is not, simplify again before committing.

Only after live VIEW pre-flight, implementation, tests, and self-review,
commit the focused change on the **current production branch**. Give one
verdict (`APPROVED`, `APPROVED WITH SMALL CHANGES`, or `NOT READY`), the
verification evidence, remaining platform assumptions, and commit SHA. Do
not claim that this isolated POC was already deployed or live-verified.
