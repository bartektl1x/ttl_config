# Production handover: parameterized decimal target types

## Task and scope

Review and repair the proposed production diff for `target_column_data_type`.
The source diff was supplied in two screenshot sets. The second set adds regex
validation, bounds checks, canonicalization, and more tests; it supersedes
some of the findings from the first set. The screenshots cover these paths:

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

## Current review of the second screenshot set

The revised `ColumnConfig` validator uses `_DECIMAL_RE.fullmatch(...)`,
checks both numeric bounds, and returns canonical `decimal(p,s)`. This is the
right narrow use of `re` and resolves the initial weak validation and
formatting findings *if* `_DECIMAL_RE` itself matches exactly the intended
grammar. Its declaration is outside the supplied screenshots: inspect the
actual pattern, verify optional spaces and two captures, and run the new
negative tests. Do not claim the regex is correct without seeing it.

The revised tests now include normalized spellings and a schema test that
asserts a nondecimal enum maps to `LongType`. This improves coverage, but the
visible schema test still uses `_mock_column` and `MagicMock` rather than a
real `ColumnConfig`. Its execution result is not shown. The production agent
must still inspect `TargetType` and run a real-model regression test.

### Remaining blocker: `str(TargetType)`

`data_vault_pipeline.py` still changes `.value.lower()` to
`str(column.target_column_data_type).lower()`. For ordinary `Enum` and
`class TargetType(str, Enum)`, `str(TargetType.STRING)` is
`"TargetType.STRING"`, not `"string"`; the mapping then rejects existing
columns. This concern does not apply if `TargetType` is genuinely `StrEnum` or
overrides `__str__` appropriately. Inspect its declaration and run a
real `ColumnConfig` -> schema-builder test. If it is an ordinary enum, use
`.value` for enum members and the canonical string for explicit decimals.

### Remaining audit: mixed enum/string consumers

The field is still `TargetType | str`. Search all reads of
`target_column_data_type`, especially `.value`, `.name`, enum comparisons,
casts, serialization, and mapping lookups. The screenshots update one
`data_vault_entity.py` call site but cannot prove complete coverage. Do not
introduce a new abstraction unless the call-site audit shows a concrete need.

### Minor cleanup: schema builder parses validated text a second time

The schema builder still extracts precision and scale with
`removeprefix(...).removesuffix(...).split(",")` and catches parse errors.
After the Pydantic validator guarantees a canonical string, this should be
evaluated against the builder's actual public contract. Either reuse the one
small parser, or consume its canonical output directly. Avoid independently
maintaining a second, broader decimal grammar or revalidating constraints
already guaranteed by `ColumnConfig`.

## Implementation direction

- Keep Pydantic as the owner of the YAML/type contract. Because the current
  model accepts both `TargetType` instances and strings, a `mode="before"`
  field validator is reasonable; do not replace it solely for stylistic
  reasons. Use a small pure decimal parser called from that validator, not a
  generic Spark SQL type parser or a new hierarchy of type classes.
- A compiled pattern with `fullmatch` and two ASCII digit captures is appropriate for
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

Do not sign off until the `_DECIMAL_RE` declaration and `TargetType` definition
have been inspected and the real-model regression test has passed.

Report the exact files changed, what was preserved, which findings were
confirmed or disproved (especially `TargetType.__str__`), and test results.
