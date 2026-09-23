# Fresh-session prompt: finish decimal target type support

You are working in the **production `shared_lib` repository**, not in this
`ttl_config` repository. Implement the change and review the entire local diff
before finishing. The change was originally shown in screenshots, which do
not expose every line or call site. Inspect actual source files and run tests;
do not treat screenshot observations as proof of behavior.

Follow the production repo's own instructions. The style references are
[principal review](https://github.com/bartektl1x/ttl_config/blob/main/skills/repo-principal-review/SKILL.md)
and [anti-overengineering](https://github.com/bartektl1x/ttl_config/blob/main/skills/ai-engineering-anti-overengineering/SKILL.md).
The applicable rules are stated here too: a small, explicit implementation,
one validation owner, and meaningful tests. Do not
add a general SQL type parser, type hierarchy, new YAML fields, or unrelated
refactoring for this feature.

## The precise user contract

The YAML field is `target_column_data_type` on `ColumnConfig`:

| YAML value | Required result |
| --- | --- |
| `decimal` | The established behavior: enum `TargetType.DECIMAL` and Spark `DecimalType(10,0)` |
| `"decimal(10, 4)"` | Spark `DecimalType(10,4)`; model stores canonical `decimal(10,4)` |
| `" DECIMAL ( 10 , 4 ) "` | The same canonical value and Spark type |
| Existing nondecimal values | Same enum representation and Spark types as before this feature |

For an explicit decimal, `p` is total digits (1 through 38), and `s` is
fractional digits (0 through `p`). Reject invalid syntax or bounds **when
constructing `ColumnConfig`**. This feature supports `decimal` and
`decimal(p,s)` only; do not silently extend it to `decimal(p)`, `numeric`,
`DEC`, negative scale, or generic Spark DDL. Bare `decimal` means the existing
default; it does not preserve precision/scale from an input CSV string.

Quote `"decimal(10, 4)"` in YAML documentation: unquoted text with a comma is
ambiguous inside a flow-style YAML mapping.

Official reference for the numeric bounds and Spark's bare-decimal default:
https://docs.databricks.com/aws/en/sql/language-manual/data-types/decimal-type

## Step 1: inspect the actual production state

Before changing code, run `git status --short` and review the complete current
`git diff` (or the feature branch/PR diff if the changes are committed).
Inspect at least these files:

- `src/shared_lib/layers/shared/config.py`
- `src/shared_lib/layers/silver/data_vault_entity.py`
- `src/shared_lib/pipelines/data_vault_pipeline.py`
- `tests/unit_tests/shared_lib/layers/shared/test_config_unit.py`
- `tests/unit_tests/shared_lib/pipelines/test_data_vault_pipeline_unit.py`

Use `rg` to find `class TargetType`, `_DECIMAL_RE`,
`target_column_data_type`, `_resolve_spark_type`, and every consumer of the
field. Check `TargetType`'s actual base class and `__str__` behavior. Identify
whether the current regex is equivalent to the grammar in Step 2. Preserve
any unrelated work already in the working tree.

The latest screenshots show `ColumnConfig` using
`_DECIMAL_RE.fullmatch(v_lower)`, checking both numeric bounds, and returning
`decimal(p,s)`. They do **not** show the `_DECIMAL_RE` declaration. They also
show the pipeline still using `str(column.target_column_data_type).lower()`;
that call deserves a concrete fix even if `TargetType` happens to be
`StrEnum`. No test result was supplied.

## Step 2: finish the Pydantic validation boundary

Retain the current field validator on `target_column_data_type`. Its
`mode="before"` is sensible because input can be either an enum member or a
YAML string. Keep existing enum members unchanged. Convert recognized plain
strings to their existing `TargetType` members. For a parameterized decimal:

1. Strip outer whitespace and lowercase the type name.
2. Require **one complete match** of a small decimal pattern with exactly two
   ASCII integer captures. Allow horizontal whitespace between `decimal` and
   `(`, and around the numbers, comma, and closing `)`.
3. Convert both captures to integers. Require `1 <= p <= 38` and
   `0 <= s <= p`. Raise `ValueError` at config validation for failures.
4. Return `f"decimal({p},{s})"`; do not preserve user spacing/case.

If the existing `_DECIMAL_RE` already has this behavior, keep it. Otherwise,
the intended shape is:

```python
_DECIMAL_RE = re.compile(
    r"decimal[ \t]*\([ \t]*([0-9]+)[ \t]*,[ \t]*([0-9]+)[ \t]*\)"
)
```

The validator uses `_DECIMAL_RE.fullmatch(normalized_value)`. There is no
need for `^...$` with `fullmatch`, no need for `re.IGNORECASE` after lowercasing,
and no need for a general DDL parser. The bounds checks are required:
PySpark 3.5's Python `DecimalType` constructor only assigns `precision` and
`scale`; it does not reject an invalid pair on construction:
https://spark.apache.org/docs/3.5.6/api/python/_modules/pyspark/sql/types.html

## Step 3: make the schema builder safe for both field variants

In `_build_schema_from_config` (or its current equivalent), convert the
validated field to a type name explicitly:

```python
configured_type = column.target_column_data_type
target_type_str = (
    configured_type.value
    if isinstance(configured_type, TargetType)
    else configured_type
).lower()
```

Do **not** use `str(configured_type).lower()`. For a conventional `Enum`,
including `class TargetType(str, Enum)`, `str(TargetType.BIGINT)` may be
`"TargetType.BIGINT"` rather than `"bigint"`. Explicit `.value` also states
the intended contract if the current class is `StrEnum`. Keep the existing
nondecimal mapping and the existing bare-decimal result `DecimalType(10,0)`.

For a canonical `decimal(p,s)`, construct `DecimalType(p,s)`. The current
`removeprefix(...).removesuffix(...).split(",")` extraction is acceptable if
only called on validated `ColumnConfig` values; do not add a new representation
class merely to avoid this small extraction. If `_resolve_spark_type` is also
an independent API accepting arbitrary strings, keep its existing contextual
`SchemaBuilderError` behavior, but do not implement a second, conflicting
decimal grammar or repeat all Pydantic validation in the schema builder.

## Step 4: audit the other consumers of `TargetType | str`

Search every read of `target_column_data_type`. Check `.value`, `.name`,
`TargetType(...)`, enum comparisons, casting, metadata creation, and
serialization. The screenshot changes one use in `data_vault_entity.py` from
`TargetType(field)` to the validated field directly; verify the rest of that
method and all other consumers. Change only actual incompatible call sites.
Keep ordinary types as enum members and explicit decimals as canonical
strings unless a demonstrated call site forces a better local representation.

## Step 5: run meaningful regression checks

Use existing fixtures and project test conventions. Test the real behavior,
not just the enum-free `_resolve_spark_type` helper or `DecimalType` objects:

1. `ColumnConfig`: enum input and bare string input remain enum members;
   `decimal(10,4)`, `DECIMAL(10, 4)`, and `decimal ( 10 , 4 )` all store the
   identical canonical `decimal(10,4)` string.
2. `ColumnConfig`: reject `decimal()`, `decimal(abc,2)`, `decimal(10)`,
   `decimal(0,0)`, `decimal(39,0)`, `decimal(10,11)`, `decimal(10,-1)`,
   `decimal(10,4,2)`, and trailing junk. Verify each fails before Spark
   schema construction. Retain existing rejection of unsupported types.
3. Build a schema from a **real validated `ColumnConfig`**, not only a
   `MagicMock`: include `TargetType.BIGINT` (expect `LongType()`), bare
   `TargetType.DECIMAL` or bare string (expect `DecimalType(10,0)`), and
   `decimal(10,4)` (expect `DecimalType(10,4)`). This catches the
   `str(enum)` regression. The mock-only test in the screenshots does not
   exercise Pydantic's mixed field representation.
4. If the entity transformation reads this field, cover its explicit-decimal
   path and its existing date/timestamp handling without changing cast
   semantics.

Run the relevant unit tests and the repository's normal lint/format gates.
If full Spark integration cannot run in your environment, say exactly which
checks ran and which need the Databricks environment. Do not claim that a
Python-side equality assertion proves Spark will accept invalid decimal
bounds.

## Step 6: final review and response

Read the final diff top-to-bottom. Explain the `TargetType` declaration you
found, the precise `_DECIMAL_RE` pattern you kept or changed, every consumer
of `target_column_data_type` that required adjustment, and how bare decimal
and nondecimal behavior were preserved. List the executed checks and results.
State any remaining limitation explicitly. Do not stop at a plan or add
unrequested abstractions. The result should be a small production patch that
passes the concrete tests above.
