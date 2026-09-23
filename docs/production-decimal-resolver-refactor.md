# Refactor: make Spark type resolution read top-down

Change only `src/shared_lib/pipelines/data_vault_pipeline.py`. Keep the existing `parse_decimal_parameters`, `ColumnConfig` validator, `TargetType`, `_TARGET_TYPE_TO_SPARK_TYPE`, and `SchemaBuilderError` definitions.

In `_build_schema_from_config`, remove the local `configured_type` / `target_type_str` conversion. Pass the validated field directly to the existing resolver call; keep its existing `field_name` and `table_name` arguments:

```python
spark_type = DataVaultPipeline._resolve_spark_type(
    column.target_column_data_type,
    field_name=field_name,
    table_name=config.target_table_name,
)
```

Replace the current `_resolve_spark_type` with these **two adjacent methods**, in this order, inside `DataVaultPipeline`:

```python
@staticmethod
def _resolve_spark_type(
    configured_type: TargetType | str,
    *,
    field_name: str,
    table_name: str,
) -> DataType:
    type_name = (
        configured_type.value
        if isinstance(configured_type, TargetType)
        else configured_type
    ).lower()

    if type_name.startswith("decimal") and type_name != "decimal":
        return DataVaultPipeline._resolve_decimal_type(
            type_name,
            field_name=field_name,
            table_name=table_name,
        )

    try:
        return _TARGET_TYPE_TO_SPARK_TYPE[type_name]
    except KeyError as error:
        raise SchemaBuilderError(
            f"Target type {type_name!r} for column {field_name!r} "
            f"in table {table_name!r} is not mapped to Spark type"
        ) from error

@staticmethod
def _resolve_decimal_type(
    type_name: str,
    *,
    field_name: str,
    table_name: str,
) -> DecimalType:
    try:
        precision, scale = parse_decimal_parameters(type_name)
    except ValueError as error:
        raise SchemaBuilderError(
            f"Invalid decimal precision/scale for column {field_name!r} "
            f"in table {table_name!r}: {error}"
        ) from error
    return DecimalType(precision, scale)
```

Indent both methods at class level. Keep the existing error wording in the `KeyError` branch if tests or callers rely on its exact text. Update any keyword call site using the old `target_type_str=` parameter name; existing positional callers continue to work.

The top-level flow is: convert the validated type to a name, resolve parameterized decimal, otherwise use the established mapping. Bare `decimal` stays in that mapping, so there is no second default branch. The decimal helper hides parsing and error translation. A separate method for dictionary lookup would only move a few obvious lines and add another jump while reading.

Run the existing config, schema builder, and resolver tests. In particular, check `TargetType.STRING`, `TargetType.DECIMAL`, `"decimal"`, `"decimal(10, 4)"`, and invalid explicit decimals passed directly to `_resolve_spark_type`. Do not add tests that only repeat those already present.
