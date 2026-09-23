# Task: implement decimal precision and scale in column configuration

Work in the Python project currently open. Complete the `target_column_data_type` feature in production code, review the full working-tree diff, and run the relevant tests. Preserve unrelated changes already present in the tree.

## Required behavior

The YAML field consumed by `ColumnConfig` is `target_column_data_type`. For example, inside an existing column definition:

```yaml
target_column_data_type: "decimal(10, 4)"
```

| Input | Value stored in `ColumnConfig` | Spark schema type |
| --- | --- | --- |
| `decimal` or `TargetType.DECIMAL` | `TargetType.DECIMAL` | `DecimalType(10, 0)` |
| `"decimal(10, 4)"` | `"decimal(10,4)"` | `DecimalType(10, 4)` |
| `" DECIMAL ( 10 , 4 ) "` | `"decimal(10,4)"` | `DecimalType(10, 4)` |
| Existing ordinary types, such as `string` | Their existing `TargetType` members | Their existing Spark types |

Precision must be 1 through 38; scale must be 0 through precision. Reject invalid syntax and bounds while constructing `ColumnConfig`, before Spark is called. Support precisely `decimal` and `decimal(p,s)`; do not add `decimal(p)`, `numeric`, or a general SQL type parser. Quote the parameterized YAML value in documentation, particularly for flow-style YAML.

## 1. Inspect the actual code before editing

Run `git status --short`, `git diff`, and, if necessary, inspect committed changes on the current branch. Read the following files and use `rg -n 'target_column_data_type|class TargetType|_resolve_spark_type|_DECIMAL_RE' src tests` to find every consumer:

- `src/shared_lib/layers/shared/config.py`
- `src/shared_lib/layers/silver/data_vault_entity.py`
- `src/shared_lib/pipelines/data_vault_pipeline.py`
- `tests/unit_tests/shared_lib/layers/shared/test_config_unit.py`
- `tests/unit_tests/shared_lib/pipelines/test_data_vault_pipeline_unit.py`
- `tests/test_utils/factories/builders/column_config_builder.py`

Check the actual `TargetType` declaration and existing enum values. Use the code below in the indicated locations; preserve surrounding fields, imports, mapping entries, and nondecimal behavior.

## 2. Parse and validate explicit decimals once

In `src/shared_lib/layers/shared/config.py`, add `import re` if missing. Keep the existing Pydantic `field_validator` import and `Any` import. Put this small parser at module scope near `ColumnConfig` (replace an existing `_DECIMAL_RE` declaration if there is one):

```python
_DECIMAL_RE = re.compile(
    r"decimal[ \t]*\([ \t]*([0-9]+)[ \t]*,[ \t]*([0-9]+)[ \t]*\)"
)


def parse_decimal_parameters(value: str) -> tuple[int, int]:
    match = _DECIMAL_RE.fullmatch(value.strip().lower())
    if match is None:
        raise ValueError("expected decimal(p,s)")

    precision = int(match.group(1))
    scale = int(match.group(2))
    if not 1 <= precision <= 38:
        raise ValueError("precision must be between 1 and 38")
    if not 0 <= scale <= precision:
        raise ValueError("scale must be between 0 and precision")
    return precision, scale
```

The regular expression recognizes the small, defined grammar and extracts both integers. `fullmatch` rejects trailing content. `[0-9]` accepts ASCII digits only. `re` belongs here because Pydantic validates field values, but does not parse this embedded two-number syntax by itself. Do not duplicate the regex or the bounds checks in the Spark pipeline.

Replace the `target_column_data_type` field annotation and its existing validator in `ColumnConfig` with the following, indented at class level. Keep every other field and validator on that model as is:

```python
target_column_data_type: TargetType | str

@field_validator("target_column_data_type", mode="before")
@classmethod
def _validate_target_column_data_type(cls, value: Any) -> TargetType | str:
    if isinstance(value, TargetType):
        return value
    if not isinstance(value, str):
        raise ValueError("target_column_data_type must be a string or TargetType")

    normalized = value.strip().lower()
    try:
        return TargetType(normalized)
    except ValueError:
        pass

    if not normalized.startswith("decimal"):
        raise ValueError(f"Unsupported target_column_data_type: {value!r}")

    try:
        precision, scale = parse_decimal_parameters(normalized)
    except ValueError as error:
        raise ValueError(f"Invalid target_column_data_type {value!r}: {error}") from error
    return f"decimal({precision},{scale})"
```

`mode="before"` makes both YAML strings and enum inputs work. Ordinary strings, including bare `decimal`, become their original enum members; parameterized decimals become canonical strings. Keep this distinction throughout the pipeline. If the model already has a validator with a different name, replace its body rather than adding a second validator for this field.

## 3. Build the correct Spark schema

In `src/shared_lib/pipelines/data_vault_pipeline.py`, import `parse_decimal_parameters` from `shared_lib.layers.shared.config` alongside the existing config imports. In `_build_schema_from_config`, replace the expression that calls `str(column.target_column_data_type).lower()` with this exact conversion:

```python
configured_type = column.target_column_data_type
target_type_str = (
    configured_type.value
    if isinstance(configured_type, TargetType)
    else configured_type
).lower()
```

Ensure `TargetType` is imported there. `str(TargetType.STRING)` can produce `"TargetType.STRING"` with ordinary `Enum`, which would break existing column types. `.value` explicitly preserves the original lookup key regardless of whether the enum subclasses `str` or uses `StrEnum`.

Keep the `"decimal": DecimalType(10, 0)` entry in `_TARGET_TYPE_TO_SPARK_TYPE`; it gives bare decimal the established `(10, 0)` behavior. At the beginning of `_resolve_spark_type`, add this branch, then leave its existing mapping lookup and unsupported-type error unchanged:

```python
if target_type_str.startswith("decimal") and target_type_str != "decimal":
    try:
        precision, scale = parse_decimal_parameters(target_type_str)
    except ValueError as error:
        raise SchemaBuilderError(
            f"Invalid decimal precision/scale for column {field_name!r} "
            f"in table {table_name!r}: {error}"
        ) from error
    return DecimalType(precision, scale)
```

This helper handles direct calls with malformed explicit decimals consistently. The `ColumnConfig` validator is still the normal validation boundary for YAML; schema construction reuses the same parser rather than inventing another grammar. Remove any separate `removeprefix(...).removesuffix(...).split(",")` decimal extraction or redundant bare-decimal branch if it exists.

## 4. Adjust consumers that assumed an enum

In `src/shared_lib/layers/silver/data_vault_entity.py`, where the code currently does `TargetType(col_config.target_column_data_type)` before comparing with `TargetType.TIMESTAMP` or `TargetType.DATE`, replace only that assignment:

```python
target_type = col_config.target_column_data_type
```

Keep the existing date and timestamp comparison and expression logic. Plain types are enum members after model validation, while explicit decimals are strings and do not belong to either date/time case.

In `tests/test_utils/factories/builders/column_config_builder.py`, change `with_data_type(self, data_type: TargetType)` to accept `TargetType | str`; retain the method body. If the backing `_data_type` attribute has a type annotation, update it to `TargetType | str` too. Search remaining consumers for `.value`, `.name`, `TargetType(...)`, and assumptions that every field value is an enum; adjust only places that actually need to accept the validated decimal string.

## 5. Add tests using real models

In `tests/unit_tests/shared_lib/layers/shared/test_config_unit.py`, use the existing imports and pytest conventions. The following is the intended test body; insert it into an appropriate test class or at module scope and add imports for `ValidationError`, `ColumnConfig`, `CalculationMode`, and `TargetType` as necessary:

```python
def make_column(configured_type: TargetType | str) -> ColumnConfig:
    return ColumnConfig(
        source_column_name="src",
        target_column_name="price",
        target_column_data_type=configured_type,
        calculation_mode=CalculationMode.DIRECT,
    )


@pytest.mark.parametrize(
    ("configured_type", "expected"),
    [
        ("decimal(10, 4)", "decimal(10,4)"),
        (" DECIMAL ( 10 , 4 ) ", "decimal(10,4)"),
        ("decimal(1,0)", "decimal(1,0)"),
        ("decimal(38,38)", "decimal(38,38)"),
    ],
)
def test_explicit_decimal_is_canonical(configured_type: str, expected: str) -> None:
    actual = make_column(configured_type).target_column_data_type
    assert actual == expected
    assert not isinstance(actual, TargetType)


@pytest.mark.parametrize("configured_type", [TargetType.DECIMAL, "decimal"])
def test_bare_decimal_remains_enum(configured_type: TargetType | str) -> None:
    assert make_column(configured_type).target_column_data_type is TargetType.DECIMAL


def test_ordinary_type_remains_enum() -> None:
    assert make_column("string").target_column_data_type is TargetType.STRING


@pytest.mark.parametrize(
    "configured_type",
    [
        "decimal()", "decimal(10)", "decimal(abc,2)", "decimal(0,0)",
        "decimal(39,0)", "decimal(10,11)", "decimal(10,-1)",
        "decimal(10,4,2)", "decimal(10,4)garbage", "uuid",
    ],
)
def test_invalid_decimal_rejected_at_config(configured_type: str) -> None:
    with pytest.raises(ValidationError):
        make_column(configured_type)
```

In `tests/unit_tests/shared_lib/pipelines/test_data_vault_pipeline_unit.py`, use the existing `_mock_config` helper and pipeline fixture, but pass **real `ColumnConfig` instances** for at least three columns. Add imports for `ColumnConfig` and `CalculationMode` as needed. The complete test body is:

```python
def test_decimal_config_builds_schema(self, mock_data_vault_pipeline) -> None:
    config = _mock_config(
        columns=[
            ColumnConfig(
                source_column_name="source_name",
                target_column_name="name",
                target_column_data_type=TargetType.STRING,
                calculation_mode=CalculationMode.DIRECT,
            ),
            ColumnConfig(
                source_column_name="source_price",
                target_column_name="price",
                target_column_data_type="decimal(10, 4)",
                calculation_mode=CalculationMode.DIRECT,
            ),
            ColumnConfig(
                source_column_name="source_default",
                target_column_name="default_decimal",
                target_column_data_type="decimal",
                calculation_mode=CalculationMode.DIRECT,
            ),
        ],
        keys=[],
        scd_options=SCDOptions(stored_as_scd_type=SCDType.TYPE_1),
        target_table_name="hub_customer",
    )
    entity = MagicMock()
    entity.build_column_comments.return_value = {}
    schema = mock_data_vault_pipeline._build_schema_from_config(config, entity)
    assert schema["name"].dataType == StringType()
    assert schema["price"].dataType == DecimalType(10, 4)
    assert schema["default_decimal"].dataType == DecimalType(10, 0)
```

Put this test inside the existing `TestBuildSchemaFromConfig` class. If the fixture/config helper requires other arguments, add only those it already expects. Retain useful tests of `_resolve_spark_type`, including a malformed decimal raising `SchemaBuilderError` with column and table context. Check any existing date/timestamp tests for the entity consumer. Where the project already tests YAML loading, add one case with the quoted value above that reaches a real `ColumnConfig`; use the existing loader and do not add a new dependency.

## 6. Finish the change

Run the focused config and pipeline unit tests, relevant entity tests, and the repository's normal formatter/linter. Inspect the final `git diff` for accidental behavior changes. In your final response, state exactly what changed, which tests and checks passed, and any checks the environment could not run. Deliver working code and tests, not just an implementation plan.
