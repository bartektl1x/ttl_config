# ttl_config

Retention configuration for Unity Catalog tables. The current package creates
rules from Excel, validates physical targets, and writes an authoritative
Delta configuration snapshot. The production PR currently uses **explicit
rules only**.

## Start here

| Path | Status | Use |
| --- | --- | --- |
| [`src/ttl_config/`](src/ttl_config/) | Current packaged baseline | Source for the explicit-rules Part 1 PR. The generator invokes its legacy column-lineage resolver when `include_inheritance=True`; that resolver is **not suitable** for the streaming workload. Keep inheritance disabled. |
| [`inheritance_v2/`](inheritance_v2/) | Isolated design POC | Table-lineage resolver and synthetic tests for Part 2. The generator does not import it, and the wheel does not package it. |
| [`inheritance_v2/PRODUCTION_PART_2_REVIEW.md`](inheritance_v2/PRODUCTION_PART_2_REVIEW.md) | Active agent handover | Findings, Databricks pre-flight SQL, and the Part 1 → Part 2 sequence. **Read this first** for the production PR. |
| [`inheritance_v2/PORT_TO_PRODUCTION.md`](inheritance_v2/PORT_TO_PRODUCTION.md) | Active implementation prompt | The focused Part 2 implementation and test contract. |
| [`archive/agent-prompts/`](archive/agent-prompts/) | Historical context | Previous investigations and task prompts; they are not instructions to execute against the current branch. |

The duplicate `ttl_config_v2/` prototype has been removed from the working
tree. It remains available in Git history. The repository's
[`engineering skills`](skills/) govern any production adaptation.

## Explicit-rule path

```python
from ttl_config.retention_config_generator import RetentionConfigGenerator

generator = RetentionConfigGenerator(spark)
rules = generator.generate_ttl_config(
    workbook_path="/Volumes/ops/config/retention.xlsx",
    sheet_name="retention_rules",
    include_inheritance=False,
)
generator.write_ttl_config(rules)
```

The configuration destination is `<ops_catalog>.retention.ttl_config`. The
generator checks an existing destination before replacing the entire rules
snapshot. See [the current design](docs/retention-design.md) for ownership and
validation boundaries. **Retention target tables may be partitioned**; the
unpartitioned check applies only to the configuration destination.

## Local verification

Install with the `databricks` and `dev` extras, then run:

```bash
python -m pytest tests inheritance_v2/tests -q
ruff check inheritance_v2
ruff format --check inheritance_v2
```

The inheritance tests use synthetic Spark DataFrames. They do not prove that
the required VIEW edges occur in the real Databricks workspace. Part 2 must
pass the live pre-flight before any production inheritance is enabled.
