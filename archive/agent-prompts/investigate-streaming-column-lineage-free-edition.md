# Databricks Free Edition diagnostic task: investigate missing streaming-table column lineage

## Read this first

Follow this task literally and sequentially.

Do not redesign the retention implementation.
Do not modify any files under `ttl_config_v2/retention_config/`.
Do not modify tests.
Do not commit production changes.

This is a diagnostic experiment only.

The problem we need to investigate is:

> For Lakeflow / Spark Declarative Pipelines streaming-table -> streaming-table dependencies, column lineage appears to contain the target table and target column, but the source table and source column are NULL.

We need evidence showing whether this is:

1. expected write-only lineage plus a separate read event,
2. incomplete `system.access.column_lineage` capture,
3. specific to streaming-table -> streaming-table flows,
4. specific to a transformation pattern,
5. a permission / Free Edition limitation,
6. or an incorrect assumption in our retention inheritance implementation.

Do not guess the answer before running the experiment.

---

# Important constraints

You are running inside a Databricks Free Edition workspace.

Free Edition may restrict system schemas and account-level administration.

Therefore:

- If `system.access.column_lineage` or `system.access.table_lineage` returns an authorization error, RECORD THE EXACT ERROR and continue.
- Do not attempt to grant yourself system-table permissions.
- Do not ask for account-console access.
- Do not stop, delete, or modify any unrelated pipeline.
- Free Edition can have pipeline quotas. If creating the test pipeline is blocked because another unrelated active pipeline consumes the quota, report that exact blocker and STOP rather than destroying unrelated resources.
- Do not substitute an ordinary `writeStream` job for Lakeflow pipelines. The reproduction must use Lakeflow / Spark Declarative Pipelines because that is the workload under investigation.

The experiment must use only temporary objects whose names start with:

```text
lineage_probe_
```

Do not use or modify existing business tables.

Do not clean up the experiment until ALL evidence has been captured and reported. At the end, provide cleanup commands separately.

---

# Expected final deliverable

At the end return one structured report containing:

1. workspace/runtime information,
2. exact test catalog/schema/pipeline/table names,
3. pipeline source code actually used,
4. pipeline update result,
5. event-log lineage results,
6. table-lineage results or exact access error,
7. column-lineage results or exact access error,
8. lineage API results or exact API error,
9. an evidence matrix for every tested target column,
10. a conclusion classified as one of:
   - `COLUMN_MAPPING_CAPTURED`
   - `READ_WRITE_SPLIT_EVENTS`
   - `TABLE_LINEAGE_ONLY`
   - `LINEAGE_API_ONLY`
   - `NO_COLUMN_LINEAGE_CAPTURED`
   - `BLOCKED_BY_PERMISSIONS`
   - `BLOCKED_BY_FREE_EDITION_LIMIT`
   - `INCONCLUSIVE`
11. implications for the existing retention inheritance resolver,
12. NO production-code changes.

Do not simply say "it works" or "it does not work".
Show the returned rows / relevant fields that prove the conclusion.

---

# Phase 1: capture workspace facts

Run the following SQL and include the complete output in the report:

```sql
SELECT
    current_user() AS current_user,
    current_catalog() AS current_catalog,
    current_schema() AS current_schema,
    current_metastore() AS current_metastore;
```

Also run:

```sql
SELECT version();
```

If `SELECT version()` is unavailable, capture the runtime/version information using the normal workspace/runtime metadata available to you.

Run:

```sql
SHOW CATALOGS;
```

Do not assume the writable catalog is `main`, `workspace`, or anything else.

Choose a non-`system` catalog where the current user can create a schema.

Call it:

```text
<TEST_CATALOG>
```

Create a unique schema:

```text
lineage_probe_<short_unique_suffix>
```

For example:

```text
lineage_probe_20260922_2030
```

Call the fully-qualified schema:

```text
<TEST_CATALOG>.<TEST_SCHEMA>
```

Create it with:

```sql
CREATE SCHEMA `<TEST_CATALOG>`.`<TEST_SCHEMA>`;
```

If schema creation fails:

1. record the exact error,
2. try a user-owned existing schema only if it is clearly safe to create temporary tables there,
3. keep the `lineage_probe_` prefix on every table,
4. do not touch unrelated tables.

---

# Phase 2: test current lineage-system-table access BEFORE creating the pipeline

Run each query independently.

## 2.1 Column lineage

```sql
SELECT *
FROM system.access.column_lineage
LIMIT 1;
```

Record either:

- SUCCESS, including the returned column names, or
- the full error class/message.

## 2.2 Table lineage

```sql
SELECT *
FROM system.access.table_lineage
LIMIT 1;
```

Again record success or exact error.

## 2.3 Lakeflow update timeline

```sql
SELECT *
FROM system.lakeflow.pipeline_update_timeline
LIMIT 1;
```

Record success or exact error.

An authorization failure here must NOT abort the reproduction.

---

# Phase 3: create deterministic seed data

Create one ordinary managed Delta table OUTSIDE the pipeline:

```sql
CREATE TABLE `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_seed` (
    id BIGINT,
    event_time TIMESTAMP,
    payload STRING
)
USING DELTA;
```

Insert deterministic rows:

```sql
INSERT INTO `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_seed`
VALUES
    (1, TIMESTAMP '2026-01-01 10:00:00', 'a'),
    (2, TIMESTAMP '2026-01-01 11:00:00', 'b'),
    (3, TIMESTAMP '2026-01-01 12:00:00', 'c');
```

Verify:

```sql
SELECT *
FROM `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_seed`
ORDER BY id;
```

Do not proceed until the three rows are visible.

---

# Phase 4: create ONE Lakeflow pipeline with controlled ST -> ST cases

Create a Lakeflow / Spark Declarative Pipelines pipeline using Python.

Configure the pipeline target/default catalog and schema to:

```text
catalog = <TEST_CATALOG>
schema  = <TEST_SCHEMA>
```

Use triggered execution, not continuous execution.

Use exactly ONE test pipeline.

Give the pipeline a name beginning with:

```text
lineage_probe_
```

If the Free Edition quota prevents creation because an unrelated active pipeline already exists:

- do not stop or delete that pipeline,
- record the exact quota/error message,
- classify the experiment `BLOCKED_BY_FREE_EDITION_LIMIT`,
- stop.

Use the following pipeline source, replacing ONLY the fully-qualified seed table placeholder.

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F


SEED_TABLE = "<TEST_CATALOG>.<TEST_SCHEMA>.lineage_probe_seed"


@dp.table(name="lineage_probe_source_st")
def lineage_probe_source_st():
    return (
        spark.readStream.table(SEED_TABLE)
        .select(
            "id",
            "event_time",
            "payload",
        )
    )


@dp.table(name="lineage_probe_passthrough_st")
def lineage_probe_passthrough_st():
    return (
        spark.readStream.table("lineage_probe_source_st")
        .select(
            "id",
            "event_time",
            "payload",
        )
    )


@dp.table(name="lineage_probe_renamed_st")
def lineage_probe_renamed_st():
    return (
        spark.readStream.table("lineage_probe_source_st")
        .select(
            "id",
            F.col("event_time").alias("renamed_event_time"),
            "payload",
        )
    )


@dp.table(name="lineage_probe_filtered_st")
def lineage_probe_filtered_st():
    return (
        spark.readStream.table("lineage_probe_source_st")
        .where(F.col("id") > 0)
        .select(
            "id",
            "event_time",
        )
    )


@dp.table(name="lineage_probe_derived_st")
def lineage_probe_derived_st():
    return (
        spark.readStream.table("lineage_probe_source_st")
        .select(
            "id",
            F.current_timestamp().alias("event_time"),
        )
    )
```

Do not change these transformations unless Databricks rejects the syntax.

If Databricks rejects the syntax:

1. preserve the semantic intent,
2. use the smallest syntax correction necessary,
3. report exactly what was changed and why,
4. do not add extra transformations.

The important controlled cases are:

```text
source_st -> passthrough_st.event_time
    expected semantic mapping:
    source_st.event_time -> passthrough_st.event_time

source_st -> renamed_st.renamed_event_time
    expected semantic mapping:
    source_st.event_time -> renamed_st.renamed_event_time

source_st -> filtered_st.event_time
    expected semantic mapping:
    source_st.event_time -> filtered_st.event_time

source_st -> derived_st.event_time
    NEGATIVE CONTROL:
    derived_st.event_time is generated by current_timestamp()
    and must NOT be interpreted as source_st.event_time lineage.
```

Run ONE successful triggered pipeline update.

Record:

- pipeline ID,
- update ID,
- update start/end timestamp,
- final update state.

Do not run repeated updates unless the first update fails or lineage has not appeared and you need one controlled retry.

---

# Phase 5: verify the generated streaming tables

Run:

```sql
SELECT * FROM `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_source_st` ORDER BY id;
SELECT * FROM `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_passthrough_st` ORDER BY id;
SELECT * FROM `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_renamed_st` ORDER BY id;
SELECT * FROM `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_filtered_st` ORDER BY id;
SELECT * FROM `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_derived_st` ORDER BY id;
```

Confirm that the first four tables preserve the expected source values.

For `derived_st.event_time`, confirm only that it contains generated timestamps.
Do NOT expect it to equal source `event_time`.

If any table does not exist or does not contain data, diagnose the pipeline before doing lineage analysis.

---

# Phase 6: query the Lakeflow event log

This step is REQUIRED even if `system.access` is unavailable.

Use one of the generated streaming tables, preferably:

```text
<TEST_CATALOG>.<TEST_SCHEMA>.lineage_probe_passthrough_st
```

First inspect recent pipeline events:

```sql
SELECT
    timestamp,
    event_type,
    level,
    origin,
    message,
    details
FROM event_log(
    TABLE(
        `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_passthrough_st`
    )
)
ORDER BY timestamp DESC
LIMIT 100;
```

Then extract `flow_definition` events:

```sql
SELECT
    timestamp,
    origin.update_id AS update_id,
    details:flow_definition.output_dataset AS output_dataset,
    details:flow_definition.input_datasets AS input_datasets,
    details:flow_definition.flow_type AS flow_type,
    details:flow_definition.schema AS flow_schema,
    details:flow_definition.explain_text AS explain_text
FROM event_log(
    TABLE(
        `<TEST_CATALOG>`.`<TEST_SCHEMA>`.`lineage_probe_passthrough_st`
    )
)
WHERE event_type = 'flow_definition'
ORDER BY timestamp DESC;
```

Do NOT attempt to parse `explain_text` into production lineage logic.

For this experiment, use it only as diagnostic evidence.

The key question is whether event-log topology clearly shows:

```text
lineage_probe_source_st -> lineage_probe_passthrough_st
lineage_probe_source_st -> lineage_probe_renamed_st
lineage_probe_source_st -> lineage_probe_filtered_st
lineage_probe_source_st -> lineage_probe_derived_st
```

Capture the exact `input_datasets` and `output_dataset` values.

---

# Phase 7: inspect system.access TABLE lineage if accessible

ONLY execute this phase if Phase 2 showed access to `system.access.table_lineage`.

Do not filter only by target.

We need both source-side and target-side records.

Run:

```sql
SELECT
    event_time,
    event_id,
    record_id,
    workspace_id,
    source_table_full_name,
    source_table_catalog,
    source_table_schema,
    source_table_name,
    source_type,
    target_table_full_name,
    target_table_catalog,
    target_table_schema,
    target_table_name,
    target_type,
    direct_access,
    entity_metadata
FROM system.access.table_lineage
WHERE
    event_time >= current_timestamp() - INTERVAL 1 DAY
    AND (
        source_table_full_name LIKE '%lineage_probe_%'
        OR target_table_full_name LIKE '%lineage_probe_%'
    )
ORDER BY event_time, event_id, record_id;
```

If the fully-qualified names are not returned in the format expected by the LIKE predicate, rerun using the individual catalog/schema/table columns.

Do not silently switch to a different test.

For every record classify it explicitly as:

```text
READ_ONLY:
    source_type IS NOT NULL
    target_type IS NULL

WRITE_ONLY:
    source_type IS NULL
    target_type IS NOT NULL

READ_AND_WRITE:
    source_type IS NOT NULL
    target_type IS NOT NULL
```

Pay special attention to records sharing:

- `event_id`
- `entity_metadata.dlt_pipeline_info.dlt_pipeline_id`
- `entity_metadata.dlt_pipeline_info.dlt_update_id`

Answer:

1. Is `source_st -> passthrough_st` present as one READ_AND_WRITE edge?
2. Or are there separate READ_ONLY and WRITE_ONLY events?
3. Do those events share an `event_id`?
4. Do they at least share pipeline/update metadata?
5. Does the table-level lineage correctly capture ST -> ST even when column-level does not?

---

# Phase 8: inspect system.access COLUMN lineage if accessible

ONLY execute this phase if Phase 2 showed access to `system.access.column_lineage`.

This is the most important query.

Again, DO NOT filter only by target table because that can hide separate read events.

Run:

```sql
SELECT
    event_time,
    event_id,
    record_id,
    workspace_id,
    source_table_full_name,
    source_table_catalog,
    source_table_schema,
    source_table_name,
    source_type,
    source_column_name,
    target_table_full_name,
    target_table_catalog,
    target_table_schema,
    target_table_name,
    target_type,
    target_column_name,
    direct_access,
    entity_metadata
FROM system.access.column_lineage
WHERE
    event_time >= current_timestamp() - INTERVAL 1 DAY
    AND (
        source_table_full_name LIKE '%lineage_probe_%'
        OR target_table_full_name LIKE '%lineage_probe_%'
    )
ORDER BY event_time, event_id, record_id;
```

If necessary, rerun using individual catalog/schema/table fields.

DO NOT immediately conclude that source lineage is absent when a row has:

```text
source_* = NULL
target_* = lineage_probe_passthrough_st
```

That row might be a WRITE_ONLY record.

Search the COMPLETE result for corresponding READ_ONLY records where:

```text
source = lineage_probe_source_st
target = NULL
```

Then group/analyze by:

1. `event_id`,
2. pipeline ID,
3. update ID,
4. event time.

For each of these expected mappings, write whether a direct READ_AND_WRITE row exists:

```text
source_st.event_time
-> passthrough_st.event_time

source_st.event_time
-> renamed_st.renamed_event_time

source_st.event_time
-> filtered_st.event_time
```

Also inspect:

```text
source_st.event_time
-> derived_st.event_time
```

but remember this is a NEGATIVE CONTROL and should not be treated as a valid column mapping because the target timestamp is independently generated.

For every lineage record classify it as READ_ONLY / WRITE_ONLY / READ_AND_WRITE using source/target nullability.

---

# Phase 9: test the workspace lineage API

Perform this phase even if `system.access` is denied, unless the API itself is unavailable.

Run from a Databricks notebook:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def read_column_lineage(
    table_name: str,
    column_name: str,
):
    return w.api_client.do(
        "GET",
        "/api/2.0/lineage-tracking/column-lineage",
        body={
            "table_name": table_name,
            "column_name": column_name,
        },
    )
```

Call it for:

```python
test_columns = [
    (
        "<TEST_CATALOG>.<TEST_SCHEMA>.lineage_probe_passthrough_st",
        "event_time",
    ),
    (
        "<TEST_CATALOG>.<TEST_SCHEMA>.lineage_probe_renamed_st",
        "renamed_event_time",
    ),
    (
        "<TEST_CATALOG>.<TEST_SCHEMA>.lineage_probe_filtered_st",
        "event_time",
    ),
    (
        "<TEST_CATALOG>.<TEST_SCHEMA>.lineage_probe_derived_st",
        "event_time",
    ),
]

for table_name, column_name in test_columns:
    print("=" * 100)
    print(table_name, column_name)
    try:
        lineage = read_column_lineage(
            table_name,
            column_name,
        )
        print(lineage)
    except Exception as error:
        print(type(error).__name__)
        print(str(error))
```

Capture the full response for every call.

Do not assume response keys.
Inspect the response you actually receive.

Specifically determine whether upstream information for the three positive-control columns includes:

```text
<TEST_CATALOG>.<TEST_SCHEMA>.lineage_probe_source_st.event_time
```

For the derived negative-control timestamp, determine whether upstream `source_st.event_time` is absent.

If the API returns 401 / 403 / 404 / unsupported errors:

- capture exact status/message,
- do not attempt privilege escalation,
- continue to the final evidence matrix.

Do not build or recommend a production dependency on this API yet.
This is diagnostic only.

---

# Phase 10: produce an evidence matrix

Create this exact matrix in the final report.

Use YES / NO / INACCESSIBLE / NOT_APPLICABLE, not vague language.

```text
Case:
1. passthrough event_time
2. renamed event_time
3. filtered event_time
4. derived current_timestamp negative control

Columns:
- target table
- target column
- expected source table
- expected source column
- event log confirms source table dependency?
- table_lineage has ST -> ST edge?
- table_lineage record shape: READ_ONLY / WRITE_ONLY / READ_AND_WRITE / MIXED
- column_lineage direct source+target mapping present?
- column_lineage source-only record present?
- column_lineage target-only record present?
- same event_id connects read/write records?
- same pipeline/update ID connects read/write records?
- lineage API reports expected upstream column?
- conclusion for this case
```

The negative-control row must explicitly say that:

```text
derived_st.event_time is generated by current_timestamp()
and therefore source_st.event_time must NOT be inferred merely because
source_st is an upstream table.
```

---

# Phase 11: decide which failure mode we actually have

Use the following decision rules.

## A. COLUMN_MAPPING_CAPTURED

Use only if positive-control mappings appear directly as:

```text
source table + source column
AND
target table + target column
```

in the same lineage mapping.

If this happens, compare the record fields to the current retention resolver filters and identify why the resolver failed to see them.

Do not modify the resolver yet.

---

## B. READ_WRITE_SPLIT_EVENTS

Use if:

- target writes are present with `source_* = NULL`,
- corresponding source reads exist separately,
- and there is strong event/update metadata tying them together.

Important:

This DOES NOT automatically prove a one-to-one column mapping.

For example, multiple source columns and multiple target columns in one event cannot safely be paired by position.

Report whether exact pass-through mapping can or cannot be recovered without guessing.

---

## C. TABLE_LINEAGE_ONLY

Use if:

- event log and/or `table_lineage` clearly capture `source_st -> target_st`,
- but positive-control column mappings are absent from `column_lineage`.

This is strong evidence that the problem is column-lineage capture rather than pipeline topology.

Do not recommend same-name column inference as automatically safe because the negative control exists specifically to demonstrate that table dependency does not imply column dependency.

---

## D. LINEAGE_API_ONLY

Use if:

- system column-lineage rows do not provide the mapping,
- but the lineage API returns the correct upstream columns.

This would suggest the API has richer or differently retained lineage than the system table.

Report it, but do not redesign production code in this task.

---

## E. NO_COLUMN_LINEAGE_CAPTURED

Use if:

- pipeline topology is proven,
- positive-control columns clearly pass through,
- neither system column lineage nor lineage API exposes the expected source-column mapping.

This is evidence of a Databricks lineage capture limitation/bug for the tested workload.

---

## F. BLOCKED_BY_PERMISSIONS

Use only if the experiment cannot obtain any useful lineage evidence because permissions prevent both system-table and API access.

Event log alone is enough to establish topology but not column mapping.

Be precise about what is proven and what remains unknown.

---

## G. BLOCKED_BY_FREE_EDITION_LIMIT

Use only if a Free Edition platform quota prevents creation/execution of the controlled Lakeflow pipeline.

Do not use this classification merely because `system.access` is inaccessible.

---

## H. INCONCLUSIVE

Use only when evidence genuinely conflicts or required artifacts cannot be interpreted.

Explain exactly what additional experiment would resolve it.

---

# Phase 12: compare against the current retention resolver assumptions

The current retention inheritance implementation expects direct column mappings from:

```text
system.access.column_lineage
```

with all of these populated on one row:

```text
source_table_catalog
source_table_schema
source_table_name
source_column_name

target_table_catalog
target_table_schema
target_table_name
target_column_name
```

It further expects:

```text
direct_access = true
source_type IN ('TABLE', 'STREAMING_TABLE')
target_type IN ('TABLE', 'STREAMING_TABLE')
```

The current algorithm cannot consume target-only/write-only rows because its frontier join is based on:

```text
source_catalog
source_schema
source_table
source_time_column
```

In your final report state explicitly whether the controlled experiment supports or disproves that data-source assumption for ST -> ST flows.

Do not edit the resolver.

---

# Phase 13: explicitly evaluate possible fallback sources, WITHOUT implementing them

Based only on observed evidence, assess these candidates:

## Option 1: keep current behavior

```text
Only inherit when direct column lineage exists.
Missing column mapping -> no inherited rule.
```

State whether this would create false negatives in the controlled ST -> ST case.

## Option 2: table/event-log fallback

Use pipeline/table topology plus matching column names.

You MUST discuss why this can create a false positive.

Use the negative-control example:

```python
F.current_timestamp().alias("event_time")
```

A target table can have the same column name even though it is not derived from the upstream source column.

Do not recommend this fallback as safe unless the evidence provides an additional trustworthy column-level guarantee.

## Option 3: lineage API

If the API returns correct mappings when the system table does not, state that clearly.

Do NOT implement it yet.

Consider:
- API stability/support,
- permissions,
- rate limits,
- how it identifies pipeline/update recency,
- whether it can replace the current latest-successful-update semantics.

## Option 4: pipeline event-log query-plan parsing

Do NOT recommend parsing `explain_text` for production unless there is no alternative.

Explain that event-log `flow_definition` is useful for table topology, but parsing query plans to reconstruct column mappings would introduce brittle implementation complexity.

---

# Phase 14: final recommendation

Give ONE recommended next engineering step based on evidence.

Examples:

```text
A. Current system-table mapping is actually present:
   fix our filter/join.

B. Column lineage is split into read/write events but not uniquely mappable:
   keep inheritance conservative for now.

C. Lineage API reliably provides the missing mapping:
   separately evaluate replacing/supplementing system-table reads with API access.

D. No source can provide column mapping for ST -> ST:
   document ST -> ST as unsupported inheritance until Databricks provides a reliable signal.

E. Permission-only problem:
   test in the enterprise workspace with the required system-table grants before changing code.
```

Do not propose a large generic lineage framework.

Prefer a false negative over an unsafe false positive for retention inheritance.

---

# Cleanup commands

DO NOT execute cleanup until after the full report is produced and the user has had a chance to inspect the experiment.

Provide commands similar to:

```sql
DROP SCHEMA `<TEST_CATALOG>`.`<TEST_SCHEMA>` CASCADE;
```

and identify the exact test pipeline that can be deleted separately.

Do not delete unrelated pipelines.

---

# Final response format

Your final answer must contain these headings exactly:

```text
1. Environment
2. Test Objects
3. Pipeline Code Used
4. Pipeline Run
5. Event Log Evidence
6. Table Lineage Evidence
7. Column Lineage Evidence
8. Lineage API Evidence
9. Evidence Matrix
10. Root Cause Classification
11. Impact on Current Retention Resolver
12. Safest Next Engineering Step
13. Cleanup Commands
```

If a source is inaccessible, do not omit the section.
Write the exact error there.

Do not change production code as part of this task.
