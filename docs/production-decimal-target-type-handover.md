# Production handover: parameterized decimal target types

## Task and scope

Review and repair the proposed production diff for `target_column_data_type`.
The source diff was supplied as eight screenshots, covering these paths:

- `src/shared_lib/layers/shared/config.py`
- `src/shared_lib/layers/silver/data_vault_entity.py`
- `src/shared_lib/pipelines/data_vault_pipeline.py`
- `tests/test_utils/factories/builders/column_config_builder.py`
- `tests/unit_tests/shared_lib/layers/shared/test_config_unit.py`
- `tests/unit_tests/shared_lib/pipelines/test_data_vault_pipeline_unit.py`

This handover lives in `ttl_config` for the production agent. The feature belongs
to the separate `shared_lib` production repository; do not add decimal logic to
the retention package. Read the production files and the full local git diff
before editing: screenshots show changed fragments, not every consumer.

Follow the repository's `skills/repo-principal-review/SKILL.md` and
`skills/ai-engineering-anti-overengineering/SKILL.md`: establish the contract,
give each validation one owner, preserve existing behavior, and keep the code
small and readable from top to bottom.

## Required behavior

1. Existing YAML `target_column_data_type: decimal` continues to resolve to
   `DecimalType(10, 0)`, as the previous Spark type mapping did. Do not interpret
   bare `decimal` as retaining the input column's precision and scale.
2. YAML `target_column_data_type: "decimal(10, 4)"` resolves to
   `DecimalType(10, 4)`. Accept case differences and inconsequential spaces,
   including `DECIMAL ( 10 , 4 )`, then store one canonical spelling such as
   `decimal(10,4)`. The same numeric type must not acquire different serialized
   configurations because of formatting.
3. Precision is 1 through 38; scale is 0 through precision. Reject malformed
   strings and out-of-range pairs *when validating the configuration*, with a
   useful error identifying the column. Examples: `decimal()`,
   `decimal(abc,2)`, `decimal(0,0)`, `decimal(39,0)`, `decimal(10,11)`,
   `decimal(10,-1)`, `decimal(10,4,2)`, and trailing content.
4. The requested contract has two forms, `decimal` and `decimal(p,s)`. Do not
   silently add `decimal(p)`, `numeric(p,s)`, or other SQL syntax unless the
   production code already supports it as part of its public contract.
5. All previously supported nondecimal types retain their behavior, including
   their `TargetType` enum representation where callers already rely on it.
6. Quote the example in documentation. In an inline YAML mapping, the comma in
   an unquoted `decimal(10, 4)` can be parsed as a YAML separator.

Databricks decimal rules:
https://docs.databricks.com/aws/en/sql/language-manual/data-types/decimal-type

Spark 3.5 Python source (its `DecimalType` constructor merely assigns `precision`
and `scale`; it does not validate the bounds):
https://spark.apache.org/docs/3.5.6/api/python/_modules/pyspark/sql/types.html

## Findings in the screenshot diff

### 1. Potential regression of every existing enum type: verify first

`data_vault_pipeline.py` changes `.value.lower()` to
`str(column.target_column_data_type).lower()`. For ordinary `Enum` and
`class TargetType(str, Enum)`, `str(TargetType.STRING)` is
`"TargetType.STRING"`, not `"string"`; the mapping then rejects existing
columns. This concern does not apply if `TargetType` is genuinely `StrEnum` or
overrides `__str__` appropriately. Inspect its declaration and prove the
behavior with a real `ColumnConfig` -> schema-builder test. Do not rely on
tests that pass only raw strings or `MagicMock` columns.

### 2. Configuration accepts invalid decimal specifications

`ColumnConfig._validate_custom_type()` accepts anything with the prefix
`decimal(` and suffix `)`. That includes malformed components and pairs
outside Spark's limits. The schema builder later splits the text and catches
some conversion errors, but `DecimalType(39,0)` or `DecimalType(1,2)` can be
constructed as Python objects without raising. Such tests may pass while the
schema fails only on the Spark/JVM side. Own grammar and numeric validation at
the configuration boundary, once.

### 3. Mixed enum/string representation is not audited

The changed field is `TargetType | str`: known types become enums, while an
explicit decimal is a string. One access was changed in
`data_vault_entity.py`, but the screenshots cannot prove that all consumers
were covered. Search all reads of `target_column_data_type`, especially
`.value`, `.name`, enum comparisons, casts, serialization, and mapping lookups.
Use one clear conversion to a canonical type name wherever consumers require
a string. Avoid scattering ad hoc `str(enum)` conversions.

### 4. Formatting survives validation and tests reward it

`DECIMAL(14, 2)` becomes `decimal(14, 2)`, retaining the space. The added unit
test asserts this exact spelling. Canonicalize the explicit form so
`decimal(14,2)`, `DECIMAL(14, 2)`, and `decimal ( 14 , 2 )` are equal in
configuration and any schema metadata or hashes.

### 5. The tests do not close the integration gap

New tests cover happy-path parser and mocked schema construction; one invalid
case is exercised only by calling `_resolve_spark_type` directly. Add focused
tests at the `ColumnConfig` boundary for grammar and limits, plus a test that
constructs a *real* `ColumnConfig` and builds a schema containing a nondecimal
enum, bare decimal, and explicit decimal. Cover the downstream entity consumer
if it accesses the union field. Validate the resulting `StructField.dataType`,
not just the Python constructor's return value for an invalid decimal pair.

## Implementation direction

- Keep Pydantic as the owner of the YAML/type contract. Because the current
  model accepts both `TargetType` instances and strings, a `mode="before"`
  field validator is reasonable; do not replace it solely for stylistic
  reasons. Use a small pure decimal parser called from that validator, not a
  generic Spark SQL type parser or a new hierarchy of type classes.
- An anchored `re.fullmatch` with two ASCII digit captures is appropriate for
  extracting `p` and `s` while allowing spaces around `(`, `,`, and `)`.
  Validate `1 <= p <= 38` and `0 <= s <= p` with ordinary comparisons.
  Return a canonical string for the explicit form. Keep known enum inputs
  compatible with existing callers.
- Reuse that one parser or its validated normalized output in the schema
  builder; keep bare decimal on its existing `DecimalType(10,0)` path. Do not
  trust the Python `DecimalType` constructor to enforce limits. Keep the
  existing nondecimal mapping and error behavior unless a proven bug requires
  a change.
- Choose the narrowest representation compatible with the actual production
  consumers. If retaining `TargetType | str`, account explicitly for its two
  variants at one well-named conversion boundary. If the complete call-site
  audit reveals that this is too invasive, propose a simpler representation
  and explain its migration impact before applying a broad rewrite.
- If the production column model already exposes separate `length` and
  `scale` input fields, check their actual contract. Do not silently let them
  disagree with `decimal(p,s)`; use one authoritative source of precision and
  scale. Do not add such fields solely for this feature.

## Acceptance checks

- Config: bare enum and bare string still yield `TargetType.DECIMAL` and the
  original default; explicit decimal is validated and canonicalized.
- Config: `decimal(10,4)`, `DECIMAL(10, 4)`, and `decimal ( 10 , 4 )` have
  identical meaning. Invalid grammar or bounds fail before Spark execution.
- Schema: build from actual validated config and confirm `DecimalType(10,4)`,
  `DecimalType(10,0)`, and at least one existing nondecimal type. Verify the
  entity transformation path if it reads the field.
- Run the relevant production tests and the project's normal lint checks.
  In the final review, distinguish verified behavior from assumptions about
  code that was not shown in the screenshots.

Report the exact files changed, what was preserved, which findings were
confirmed or disproved (especially `TargetType.__str__`), and test results.
