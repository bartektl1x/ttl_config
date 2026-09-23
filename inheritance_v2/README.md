# Table-lineage retention inheritance POC

This folder contains an isolated design POC. The production-facing
`src/ttl_config/` package and its caller are unchanged. The POC imports the
existing `RetentionRule` and `TableName` domain models, but is not included in
the package built from `src/`.

The resolver follows direct `system.access.table_lineage` edges from explicit
rule tables through TABLE, STREAMING_TABLE, and VIEW nodes in Bronze/Silver
catalogs. A VIEW carries a policy through the graph but never becomes a final
retention target. PATH and MATERIALIZED_VIEW do not enter the graph. Each
inherited policy keeps its source `time_column` and `expiration_days` exactly;
the existing `RetentionTargetValidator` remains responsible for validating
physical downstream columns when this design is ported.

The SQL path selects only the latest completed REFRESH or FULL_REFRESH update
per `(workspace_id, pipeline_id)`. Traversal collects only reachable edges,
subject to the existing `max_mappings` budget. Explicit rules win. Identical
candidates collapse; conflicting candidates do not propagate. Spark lineage
failures return the explicit rules.

## Verification

After installing this repository with its `databricks` and `dev` extras, run:

```bash
python -m pytest inheritance_v2/tests -q
ruff check inheritance_v2
ruff format --check inheritance_v2
```

The integration tests use local Spark DataFrames shaped like the Databricks
system tables. They verify the query and traversal logic but **cannot prove**
that a real Lakeflow update emits both direct edges of a
`STREAMING_TABLE -> VIEW -> STREAMING_TABLE` chain. That pre-flight is mandatory
before this design is implemented in a production repository. See
[`PORT_TO_PRODUCTION.md`](PORT_TO_PRODUCTION.md) for the exact porting task.
