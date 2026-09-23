# ttl_config

Production-oriented retention configuration for Unity Catalog tables.

This repository contains the first consolidated version of the retention configuration prototype previously spread across Wine-Quality and logger.

## Scope

The library:

1. reads explicit retention rules from a product-owned Excel worksheet,
2. optionally inherits rules through the legacy Lakeflow column-lineage resolver,
3. validates target tables and time columns against Unity Catalog,
4. writes an authoritative Delta snapshot of the effective configuration.

Explicit rules remain authoritative. Inheritance defaults to disabled. The
legacy resolver is still callable when explicitly enabled, but its column-lineage
assumption does not hold for the target streaming workload. Keep inheritance
disabled in production until the table-lineage design in `inheritance_v2/` is
verified and ported. See [the Part 2 review and plan](inheritance_v2/PRODUCTION_PART_2_REVIEW.md).

## Repository layout

    src/ttl_config/
      retention_rules.py
      excel_retention_rules.py
      retention_rule_inheritance.py
      retention_target_validator.py
      retention_config_generator.py
    skills/
      repo-principal-review/SKILL.md
      ai-engineering-anti-overengineering/SKILL.md
    docs/
      retention-design.md
    tests/
      test_retention_rules.py

## Example

    from ttl_config.retention_config_generator import RetentionConfigGenerator

    generator = RetentionConfigGenerator(spark)

    rules = generator.generate_ttl_config(
        workbook_path="/Volumes/ops/config/retention.xlsx",
        sheet_name="retention_rules",
    )

    generator.write_ttl_config(rules)

The generated destination is read from the Spark configuration key ops_catalog and is written to:

    <ops_catalog>.retention.ttl_config

## Deliberate boundaries

- The Excel reader owns workbook shape, parsing, row limits, and duplicate explicit targets.
- Pydantic models own local rule and Unity Catalog identifier invariants.
- The target validator owns Unity Catalog compatibility.
- The inheritance resolver owns lineage-based enrichment only.
- The generator owns orchestration and the authoritative output snapshot.
- The implementation relies on the Lakeflow/SDP DAG and current successful-update contracts; it does not reconstruct historical ownership or failed-update state.

## Development

    python -m pytest

The Spark-dependent components are intended to run in Databricks or an environment with the matching PySpark and Databricks runtime dependencies.
