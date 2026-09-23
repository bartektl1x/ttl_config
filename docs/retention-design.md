# Retention configuration design

## Effective configuration flow

    Excel worksheet
        -> explicit RetentionRule models
        -> optional inheritance (disabled by default)
        -> Unity Catalog target validation
        -> authoritative Delta snapshot

## Explicit rules

The workbook contains exactly one row per target table with:

- catalog
- schema
- table
- time column
- expiration days

The workbook reader rejects missing, unexpected, or duplicate columns; empty worksheets; duplicate target tables; malformed identifiers; invalid time columns; and non-integral expiration values.

## Inheritance

The currently wired resolver starts from explicit rules and follows direct
top-level column mappings exposed by `system.access.column_lineage`. The
generator defaults to `include_inheritance=False`; production should leave it
disabled for the streaming workload because its column mappings are unreliable.
`inheritance_v2/` is an isolated table-lineage alternative and is not wired
into the packaged generator. See
[`inheritance_v2/PRODUCTION_PART_2_REVIEW.md`](../inheritance_v2/PRODUCTION_PART_2_REVIEW.md)
for the deployment gates and Part 2 plan.

Only mappings associated with the latest completed REFRESH or FULL_REFRESH update of each pipeline are considered. A rule is inherited only when the target receives one effective policy. Explicit rules always win.

The resolver intentionally does not:

- reconstruct historical mappings
- recover failed updates
- reconcile ownership migrations
- infer nested-field lineage
- manufacture a policy from ambiguous candidates

The lineage graph is expected to be acyclic under the Lakeflow/SDP contract. The resolver collects only the reachable mappings and enforces a driver-memory mapping budget.

## Target validation

The validator checks configured tables using Unity Catalog information schema metadata:

- managed Delta and Iceberg tables are supported
- streaming tables backed by Delta are supported
- the configured top-level time column must exist
- the time column type must be DATE, TIMESTAMP, or TIMESTAMP_NTZ

## Output

The generator writes a complete, unpartitioned managed Delta snapshot to:

    <ops_catalog>.retention.ttl_config

When the destination already exists, its type, format, partitioning, and exact schema are validated before overwrite. The generated snapshot is authoritative: omitted rules are removed from the configuration table.
