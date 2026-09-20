# ttl_config

Production-oriented retention configuration for Unity Catalog tables.

This repository contains the first consolidated version of the retention configuration prototype previously spread across Wine-Quality and logger.

## Scope

The library:

1. reads explicit retention rules from a product-owned Excel worksheet,
2. optionally inherits rules through current successful Lakeflow column lineage,
3. validates target tables and time columns against Unity Catalog,
4. writes an authoritative Delta snapshot of the effective configuration.

Explicit rules remain authoritative. Lineage inheritance is optional enrichment: ambiguous or unavailable lineage produces no derived rule.

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

    rules = generator.generate_retention_config(
        workbook_path="/Volumes/ops/config/retention.xlsx",
        sheet_name="retention_rules",
        include_inheritance=True,
    )

    generator.write_retention_config(rules)

The generated destination is read from the Spark configuration key ops_catalog and is written to:

    <ops_catalog>.retention.retention_config

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
