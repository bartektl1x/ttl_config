# Agent task: simplify retention inheritance to table-level lineage

## READ THIS FIRST

You are working on the user's REAL current branch, not on the historical POC.

The POC repository / old implementation may be useful as context, but the CURRENT CHECKED-OUT BRANCH is the source of truth.

Do NOT copy an older `retention_rule_inheritance.py` wholesale over the current branch.

Before editing anything:

1. locate the current `RetentionRuleInheritanceResolver`,
2. locate its current unit tests,
3. locate its current integration tests,
4. inspect all current callers,
5. note any branch-specific changes compared with older POC versions,
6. preserve those unrelated changes.

This task changes ONLY the inheritance model.

Do not redesign Excel parsing, environment catalog resolution, target validation, output writing, packaging, or unrelated retention behavior.

The desired result is a SMALLER resolver than the current column-lineage implementation.

---

# 1. Business decision

We have experimentally confirmed in Databricks that streaming-table -> streaming-table COLUMN lineage is not reliable for this workload.

The current implementation must therefore stop depending on:

```text
system.access.column_lineage
```

for retention inheritance.

The new inheritance contract is:

> Table lineage determines WHERE a retention policy propagates.
> The retention time-column name itself never changes during inheritance.

Example:

```text
bronze.orders.event_time, 30 days
        |
        v
silver.orders.event_time, 30 days
        |
        v
silver.orders_view
        |
        v
silver.orders_scd2.event_time, 30 days
```

The inherited policy always keeps:

```text
time_column = source_rule.time_column
expiration_days = source_rule.expiration_days
```

There is NO attempt to infer:

```text
event_time -> created_at
event_time -> renamed_event_time
```

If a downstream physical retention target does not contain the same time-column name, the existing `RetentionTargetValidator` must reject the generated rule later.

That is intentional.

Do not add column-name guessing.

Do not parse Spark plans.

Do not use column lineage as a fallback.

Do not maintain two inheritance algorithms.

---

# 2. Real production topology

The user's real logical flow is approximately:

```text
CSV / Volume
    ->
Bronze Streaming Table
    ->
Streaming Table
    ->
View
    ->
SCD2 Streaming Table
    ->
Materialized View
```

For retention inheritance:

```text
CSV / PATH
    irrelevant

TABLE / STREAMING_TABLE
    lineage node
    AND valid candidate for inherited retention output

VIEW
    lineage node only
    NEVER add a retention rule for the view itself
    BUT policy must be allowed to pass through it

MATERIALIZED_VIEW
    OUT OF SCOPE
    do not create retention rules for it
    do not traverse through it

PATH
    OUT OF SCOPE
```

The expected useful propagation is therefore:

```text
Bronze ST
   -> Silver ST
   -> VIEW
   -> SCD2 ST

policy reaches the SCD2 ST,
but no RetentionRule is emitted for the VIEW.
```

The final materialized view is intentionally ignored.

---

# 3. Mandatory pre-flight check for VIEW traversal

Before implementing the redesign, verify in the current Databricks workspace that table lineage correctly captures the VIEW bridge used by this workload.

Prefer inspecting an existing safe production-like pipeline/update rather than creating unrelated infrastructure.

Use `system.access.table_lineage`.

Find one known chain equivalent to:

```text
STREAMING_TABLE -> VIEW -> STREAMING_TABLE
```

For the same relevant successful pipeline update, confirm that table lineage contains enough direct read+write edge information to reconstruct BOTH edges:

```text
upstream table -> view
view -> downstream streaming table
```

Inspect at least:

```text
source_table_catalog
source_table_schema
source_table_name
source_type

target_table_catalog
target_table_schema
target_table_name
target_type

direct_access
workspace_id
entity_metadata.dlt_pipeline_info.dlt_pipeline_id
entity_metadata.dlt_pipeline_info.dlt_update_id
event_time
event_id
```

Expected types are:

```text
TABLE
STREAMING_TABLE
VIEW
```

Do NOT rely on `MATERIALIZED_VIEW`.

### Decision

If VIEW traversal is clearly represented:

```text
PROCEED WITH IMPLEMENTATION
```

If the required VIEW edges are NOT represented reliably:

```text
STOP
DO NOT INVENT A FALLBACK
DO NOT MODIFY PRODUCTION CODE
REPORT THE EVIDENCE
```

The rest of this prompt assumes the VIEW pre-flight succeeds.

---

# 4. Catalog scope

Inheritance is intentionally restricted to Bronze and Silver catalogs only.

A catalog is in scope when its catalog NAME contains, case-insensitively:

```text
bronze
```

or:

```text
silver
```

Examples in scope:

```text
dev_bronze
qat_bronze
uat_bronze
prd_bronze

dev_silver
qat_silver
uat_silver
prd_silver

company_dev_bronze_data
company_silver
```

Do NOT hardcode a specific environment prefix.

Do NOT require the catalog to equal exactly `bronze` or `silver`.

Use the existing physical catalog names from lineage.

For a lineage edge to be considered, BOTH:

```text
source_table_catalog
target_table_catalog
```

must independently contain either `bronze` or `silver`, case-insensitively.

Conceptually:

```text
in_scope(source_catalog)
AND
in_scope(target_catalog)
```

where:

```text
in_scope(catalog)
=
lower(catalog) contains "bronze"
OR
lower(catalog) contains "silver"
```

Do not add Gold catalogs.

Do not add arbitrary catalog configuration for this task.

The purpose of this filter is to reduce the lineage graph to the known retention domain and keep the resolver simple.

---

# 5. Required lineage source

Replace column lineage with:

```text
system.access.table_lineage
```

The resolver must no longer read:

```text
system.access.column_lineage
```

Remove all implementation concepts that exist only because of column-level lineage.

Examples that should disappear if they exist in the current branch:

```text
source_time_column in lineage mappings
target_time_column in lineage mappings
_TimeColumn
_TimeColumnLineage
column frontier keys
column mapping parsing
target-column trimming checks
column rename propagation
column-lineage-specific tests
```

Do not leave dead compatibility code behind.

---

# 6. Keep the latest-successful-update protection

Do NOT simplify away the existing protection against stale historical lineage.

`system.access.table_lineage` is historical event data.

A removed dependency from an older pipeline update must not continue to produce an inherited retention policy.

Therefore preserve the branch's existing concept of:

```text
latest successful relevant pipeline update
```

and join table-lineage rows to that update identity.

In the POC this was based on:

```text
workspace_id
pipeline_id
update_id
```

from:

```text
system.lakeflow.pipeline_update_timeline
```

with the latest successful relevant refresh selected per:

```text
workspace_id
pipeline_id
```

If the real branch already has a refined version of this logic, KEEP the current branch behavior.

Do not regress to using all historical lineage rows.

Do not add historical fallback.

Do not use failed updates.

Do not redesign update ownership unless a concrete current-branch bug requires it.

---

# 7. Direct table-lineage filtering

The direct lineage acquisition should conceptually retain only rows satisfying ALL relevant conditions:

```text
current metastore

direct_access = true

source_type IN (
    'TABLE',
    'STREAMING_TABLE',
    'VIEW'
)

target_type IN (
    'TABLE',
    'STREAMING_TABLE',
    'VIEW'
)

source catalog contains bronze or silver

target catalog contains bronze or silver

row belongs to the latest successful relevant pipeline update
```

Because `MATERIALIZED_VIEW` is not in the allowed type list, it is automatically excluded.

Because `PATH` is not in the allowed type list, CSV/Volume input is automatically excluded.

Do not add separate special-case branches for PATH or MATERIALIZED_VIEW if the type filter already removes them.

Prefer deleting code to adding exclusions after the fact.

---

# 8. New graph model

The lineage graph must become TABLE-level.

Prefer the simplest clear representation compatible with the current branch.

Conceptually:

```python
type _TableLineage = dict[TableName, set[TableName]]
```

or an equivalently small representation.

The graph means only:

```text
source table/view -> downstream table/view
```

It must NOT carry a source or target column.

You may additionally need a very small set identifying VIEW nodes, for example:

```python
view_tables: set[TableName]
```

This is acceptable if it is the simplest way to support:

```text
propagate THROUGH view
but do not EMIT RetentionRule for view
```

Do NOT introduce:

```text
LineageGraph class
Node class
Edge class
PolicyState class
Repository
Service
Manager
Strategy
Factory
generic graph framework
```

Use plain built-in collections.

---

# 9. Reachability

Keep inheritance limited to lineage reachable from explicit retention-rule tables.

Do not build a generic account-wide graph in Python unless the CURRENT branch already has a clearly better bounded implementation.

The preferred behavior remains:

```text
start from explicit rule tables
follow downstream table dependencies
stop when no new reachable downstream nodes remain
```

If the current branch has a mapping/dependency safety budget, preserve the safety property.

If the current public constructor exposes a parameter such as:

```text
max_mappings
```

do NOT rename/remove that public argument merely for aesthetics unless there are no callers and the repo conventions clearly permit the API change.

Internal terminology may say `dependencies` rather than `column mappings`.

The objective is simplification without unnecessary API churn.

---

# 10. Policy propagation

This is the most important semantic change.

For any selected source policy:

```python
source_rule.time_column
source_rule.expiration_days
```

the downstream candidate keeps exactly those values.

Conceptually:

```python
candidate = RetentionRule(
    table_name=downstream_table,
    time_column=source_rule.time_column,
    expiration_days=source_rule.expiration_days,
)
```

There is NO `target_time_column`.

There is NO column mapping.

There is NO rename inference.

Because `source_rule` is already validated and `downstream_table` came from a valid table identity, do not preserve obsolete defensive validation that only existed because an arbitrary physical target column came from `column_lineage`.

In particular, the old POC helper:

```text
_inherit_rule(source_rule, target_table, target_time_column)
```

and its special handling for Pydantic trimming of `target_time_column` should disappear.

Ask:

> What supported scenario still requires this code after target_time_column no longer exists?

If none, delete it.

---

# 11. VIEW behavior

A VIEW is a transparent lineage node for policy propagation.

Example:

```text
A STREAMING_TABLE
    |
    v
V VIEW
    |
    v
B STREAMING_TABLE
```

with:

```text
A -> explicit event_time / 30
```

must produce effective output:

```text
A -> event_time / 30
B -> event_time / 30
```

It must NOT return:

```text
V -> event_time / 30
```

as a final RetentionRule.

However, internally the policy must pass through V so that B receives it.

Keep this implementation simple.

Do not send the view to `RetentionTargetValidator` as an inherited retention target.

Do not create a second graph solely for views.

---

# 12. MATERIALIZED_VIEW behavior

Materialized views are intentionally excluded from this inheritance feature.

If lineage contains:

```text
SCD2_STREAMING_TABLE -> MATERIALIZED_VIEW
```

the materialized-view edge must not enter the supported lineage graph.

No RetentionRule for the MV.

No traversal through the MV.

No special downstream recovery.

This is a deliberate product boundary.

---

# 13. Explicit rules remain authoritative

Preserve existing behavior:

```text
explicit rule wins
```

Example:

```text
A explicit: event_time / 30
A -> B
B explicit: event_time / 90
B -> C
```

Expected:

```text
A = event_time / 30
B = event_time / 90
C = event_time / 90
```

Do not allow inherited policy to override explicit policy.

---

# 14. Ambiguity behavior remains conservative

Preserve the current conservative contract.

Example:

```text
A explicit: event_time / 30 ----\
                                > C
B explicit: event_time / 90 ----/
```

C has conflicting incoming policies.

Expected:

```text
no inherited rule for C
```

and policy from that ambiguous C must not propagate further.

Equal effective policies must still collapse.

Example:

```text
A: event_time / 30 ----\
                       > C
B: event_time / 30 ----/
```

Expected:

```text
C inherits event_time / 30 once
```

Because the time column no longer changes along lineage, candidate identity remains naturally:

```text
(time_column, expiration_days)
```

or the current branch's equivalent.

Do not weaken ambiguity safety.

---

# 15. Independent paths after ambiguity

Preserve current behavior where an ambiguous branch does not poison unrelated valid branches.

Example:

```text
A 30 --\
       > B -> C
X 90 --/

D 30 ------> C
```

If B is ambiguous, B must not propagate.

But D's independent unambiguous policy may still allow C to inherit according to the existing resolver semantics.

Preserve the current tested contract.

---

# 16. Target validation remains the physical-column safety check

Do not add a new Spark schema lookup to the inheritance resolver.

The existing orchestration already validates effective retention targets after inheritance.

Therefore if:

```text
bronze.event_time
    ->
silver.created_at
```

but the inheritance contract keeps:

```text
event_time
```

the generated downstream rule becomes:

```text
silver.event_time
```

and `RetentionTargetValidator` should reject it because the physical column does not exist.

This is intentional.

Do NOT teach inheritance to inspect target schemas.

Do NOT move target-column validation into inheritance.

One invariant, one owner.

---

# 17. Spark failure behavior remains optional-enrichment behavior

Preserve the existing high-level failure contract:

```text
Spark / system-table failure while obtaining optional lineage
    ->
log warning
    ->
return explicit rules only
```

Do not introduce a SQLSTATE taxonomy.

Do not distinguish dozens of system-table failure categories unless the current branch already has materially different behavior that must be preserved.

Lineage inheritance remains optional enrichment.

---

# 18. Simplification target

This task is NOT:

> replace the word column with table while keeping the same amount of code.

Actively delete obsolete complexity.

Specifically challenge and preferably remove:

```text
_TimeColumn
_TimeColumnLineage
source_time_column lineage fields
target_time_column lineage fields
column-specific frontier
column mapping parser
column-validation workaround
_inherit_rule helper if it no longer has meaningful work
tests that exist only for renamed/malformed inferred target columns
column-lineage integration fixture fields
```

Keep methods only when they represent a meaningful algorithmic or Spark boundary.

A coherent 25-40 line traversal method is acceptable.

Do not split it into tiny helpers merely to reduce line count.

The desired reaction after the refactor is:

> Given the contract "same column name downstream", this is the obvious implementation.

---

# 19. Suggested conceptual method hierarchy

Do NOT mechanically force these exact names if the current real branch has better established names.

But the final file should read approximately like:

```text
inherit_retention_rules
    _read_lineage
        _read_current_dependencies
            _read_direct_table_dependencies
            _read_latest_completed_updates
        _collect_reachable_lineage
            _read_frontier_dependencies
            _parse_table_dependencies
    _resolve_inherited_rules
        _order_downstream_tables
    _combine_retention_rules
```

If some helpers become trivial after the redesign, MERGE or DELETE them.

Do not preserve a helper just because it existed in the POC.

---

# 20. Unit-test redesign

Update the CURRENT branch tests; do not copy old POC tests wholesale.

Delete or rewrite tests that prove obsolete column-mapping behavior.

The unit suite must prove at least these business decisions.

## 20.1 Transitive propagation preserves same column

```text
A -> B -> C
A explicit event_time / 30

=> B event_time / 30
=> C event_time / 30
```

## 20.2 Explicit override propagates

```text
A -> B -> C
A explicit event_time / 30
B explicit event_time / 90

=> C event_time / 90
```

## 20.3 Equal candidates collapse

Two upstream rules:

```text
event_time / 30
event_time / 30
```

into the same target produce one inherited policy.

## 20.4 Conflicting candidates are rejected

Conflicting expiration or time-column policies into one target produce no inherited rule there.

## 20.5 Ambiguous intermediate stops propagation

Ambiguous B must not propagate a policy into C.

## 20.6 Independent path survives ambiguity

Preserve the current business behavior for a separate valid path into a downstream table.

## 20.7 VIEW is traversal-only

Graph:

```text
A STREAMING_TABLE
   ->
V VIEW
   ->
B STREAMING_TABLE
```

A has explicit `event_time / 30`.

Expected returned rules:

```text
A event_time / 30
B event_time / 30
```

No final rule for V.

This test is required.

## 20.8 Spark lineage failure falls back to explicit rules

Preserve existing behavior.

## 20.9 Mapping/dependency budget

If the current implementation retains the budget, preserve one useful budget-overflow test.

## 20.10 Invalid budget constructor input

Preserve only if the constructor still exposes the current budget argument.

---

# 21. Tests that should disappear if they exist only for column lineage

Tests equivalent to these should be removed rather than translated artificially:

```text
target column renamed from EventTime to eventtime
target physical column trimming changed by Pydantic
nested target column mapping
column-level source frontier matching
source_column_name / target_column_name parsing
```

They no longer represent supported behavior.

Do not keep obsolete tests for historical symmetry.

---

# 22. Integration-test redesign

The integration suite should validate Spark/system-table query construction and filtering, not duplicate the unit policy algorithm.

Adapt the current inheritance integration tests to use rows shaped like:

```text
system.access.table_lineage
```

NOT `column_lineage`.

At minimum prove:

1. latest successful relevant update is selected,
2. failed later update is ignored,
3. older successful update does not leak through,
4. another metastore is ignored,
5. `direct_access = false` is ignored,
6. `PATH` is ignored,
7. `MATERIALIZED_VIEW` is ignored,
8. TABLE is allowed,
9. STREAMING_TABLE is allowed,
10. VIEW is allowed for traversal,
11. catalog outside Bronze/Silver is ignored,
12. Bronze -> Silver is allowed,
13. Silver -> VIEW -> Silver dependencies remain available,
14. reachable-frontier behavior works with table identity only.

Do NOT include fake `source_column_name` / `target_column_name` fields in the new table-lineage fixture merely because the old integration fixture had them.

---

# 23. Exact Bronze/Silver integration examples

Include representative catalog names such as:

```text
dev_bronze
dev_silver
dev_gold
shared_reference
```

Expected:

```text
dev_bronze -> dev_bronze       keep
dev_bronze -> dev_silver       keep
dev_silver -> dev_silver       keep
dev_silver -> VIEW in dev_silver keep

dev_silver -> dev_gold         drop
shared_reference -> dev_silver drop
dev_silver -> shared_reference drop
```

Also prove case-insensitive matching with at least one uppercase/mixed-case catalog value in the synthetic lineage rows.

Do not normalize catalog scope using environment-specific hardcoding.

---

# 24. Production query requirements

When reading `system.access.table_lineage`, select only fields actually required by the algorithm.

Do not carry unused event metadata through the DataFrame after it has served filtering/join purposes.

The selected dependency representation should be approximately:

```text
source_catalog
source_schema
source_table
source_type

target_catalog
target_schema
target_table
target_type

workspace_id
pipeline_id
update_id
```

Then after the latest-update join, keep only what downstream traversal needs.

If VIEW detection can be represented more simply, use the simpler representation.

Do not select column names.

---

# 25. Catalog filter implementation guidance

Prefer a simple Spark expression.

Conceptually:

```python
def in_retention_catalog(column):
    catalog = F.lower(column)
    return catalog.contains("bronze") | catalog.contains("silver")
```

Do not create a public utility module for this.

If a tiny local expression is clearer than a helper, keep it local.

Apply it independently to source and target catalogs.

Do not use regex unless it materially improves clarity.

---

# 26. Validation ownership

Keep ownership exactly separated:

```text
Excel reader
    workbook/input validation

RetentionRule / TableName
    local domain invariants

Inheritance resolver
    table topology + policy propagation only

RetentionTargetValidator
    physical target existence/type/time-column validation

Generator
    orchestration
```

Do not add target table-type/schema validation into inheritance.

The resolver may know VIEW only to avoid emitting a final rule for it while still traversing it.

That is lineage behavior, not target validation.

---

# 27. Do not introduce new configuration unless necessary

Do NOT add:

```text
allowed_catalog_patterns config
allowed_lineage_types config
view_behavior config
materialized_view_behavior config
same_column_name flag
```

These are fixed feature contracts for now.

Hardcode the small domain contract locally in the resolver.

Future configurability is out of scope.

---

# 28. Before editing: summarize the planned deletion

Before making code changes, print a short plan identifying which CURRENT code becomes obsolete because column lineage is being removed.

It should include concrete current symbols/methods, not generic prose.

Example only:

```text
DELETE/REPLACE:
- _TimeColumn
- _TimeColumnLineage
- source_time_column/target_time_column mapping fields
- _read_direct_column_mappings -> table dependency reader
- column-specific frontier
- _inherit_rule target-column validation
- obsolete column-mapping tests

KEEP:
- latest successful update selection
- optional-enrichment Spark failure fallback
- ambiguity semantics
- explicit override
- topological ordering
- mapping/dependency safety budget
```

Adapt this list to the REAL current branch.

---

# 29. After implementation: principal-level self-review

Before committing, perform this review.

For every remaining substantial method ask:

```text
What supported scenario requires this method?
Can it be deleted?
Can it be merged without making the parent harder to read?
Does it still contain column-lineage concepts accidentally?
```

Then explicitly search the modified inheritance code/tests for:

```text
column_lineage
source_time_column
target_time_column
_TimeColumn
_TimeColumnLineage
```

Expected result in the inheritance feature:

```text
none
```

unless one appears only in a historical comment explaining why column lineage was removed.
Prefer no historical implementation comments in production code.

Also search for:

```text
MATERIALIZED_VIEW
```

It may appear in tests proving exclusion, but production should ideally exclude it naturally by allowing only TABLE / STREAMING_TABLE / VIEW.

---

# 30. Required verification

Run the smallest relevant suite first:

```text
inheritance unit tests
inheritance integration tests
```

Then run the broader retention suite available in the current branch.

If the environment cannot run integration tests, say exactly why.

Do not claim tests passed if they were not executed.

Also run static/syntax checks used by the real repo.

Show:

1. files changed,
2. tests executed,
3. tests passed/failed,
4. any tests skipped and why,
5. production LOC before/after for the inheritance file,
6. methods removed,
7. methods added,
8. confirmation that `system.access.column_lineage` is no longer used.

The new production resolver should preferably be materially shorter than the previous implementation.

If it becomes the same size or larger, stop before committing and perform another simplification pass.

---

# 31. Acceptance criteria

The implementation is complete only if ALL are true:

1. VIEW pre-flight succeeded.
2. Inheritance reads `system.access.table_lineage`, not `column_lineage`.
3. No column mapping is used.
4. Inherited `time_column` is copied unchanged from the source policy.
5. Inherited `expiration_days` is copied unchanged.
6. TABLE is supported as a lineage/retention node.
7. STREAMING_TABLE is supported as a lineage/retention node.
8. VIEW is supported as a traversal-only node.
9. No final RetentionRule is emitted for VIEW.
10. MATERIALIZED_VIEW is not traversed and receives no policy.
11. PATH is not traversed.
12. Both source and target catalog names must contain `bronze` or `silver`, case-insensitively.
13. Gold and unrelated catalogs are excluded.
14. Explicit rules remain authoritative.
15. Equal policies collapse.
16. Conflicting policies remain conservative/ambiguous.
17. Ambiguous intermediates do not propagate.
18. Independent valid paths continue to work.
19. Latest-successful-update filtering remains.
20. Old/failed pipeline updates do not leak stale dependencies.
21. Spark/system-table failure falls back to explicit rules only.
22. Existing target validator remains responsible for checking that the copied time column actually exists downstream.
23. No schema lookup is added to inheritance.
24. No column-lineage fallback remains.
25. No generic graph abstraction is introduced.
26. Resolver production code is materially simpler/shorter than before.
27. Tests reflect the new table-level contract rather than preserving obsolete column-level scenarios.

---

# 32. Commit behavior

Only after:

- VIEW pre-flight passes,
- implementation is complete,
- relevant tests pass or unavailable tests are explicitly explained,
- self-review is complete,

commit the focused change to the CURRENT checked-out branch.

Use a focused commit message such as:

```text
refactor: simplify retention inheritance to table lineage
```

Do not include unrelated cleanup.

Do not modify unrelated files just because you notice other opportunities.

---

# 33. Final report format

Return exactly these sections:

```text
1. VIEW Pre-flight Evidence
2. Current-Branch Differences Preserved
3. Design Applied
4. Code Deleted
5. Code Added
6. Bronze/Silver Filtering
7. VIEW and Materialized View Behavior
8. Tests Updated
9. Verification Results
10. LOC / Complexity Change
11. Remaining Assumptions
12. Commit
```

In section 11 explicitly state this invariant:

```text
Retention inheritance requires the retention time-column name to remain unchanged downstream.
If the downstream physical table does not contain that column, RetentionTargetValidator rejects the effective configuration.
```

Do not propose additional abstractions after completing the task.
