# Final cleanup of decimal target-column types

Work on the current production branch. This is a focused follow-up to an implementation already present: `ColumnConfig` accepts bare `decimal` and explicit `decimal(p,s)`, the shared parser validates and canonicalizes the explicit form, and `DataVaultPipeline` has separate `_resolve_spark_type` and `_resolve_decimal_type` methods. Preserve these working parts. Review the **complete** current diff and repository conventions before changing anything; do not rebuild the feature or duplicate its tests.

## 1. Use a shared-library error without breaking Pydantic

Inspect `src/shared_lib/errors/errors.py` and the existing exception hierarchy. For invalid target-column types, reuse an existing, semantically appropriate shared-library exception **only if it subclasses `ValueError`**. Otherwise add one small exception to that module (and export it only if the package normally exports its errors):

```python
class InvalidTargetColumnDataTypeError(ValueError):
    """Raised when a configured target-column data type is invalid."""
```

Import the selected error from `shared_lib.errors.errors` in `src/shared_lib/layers/shared/config.py`. Use it for the explicit `raise ValueError(...)` paths in `parse_decimal_parameters` and `ColumnConfig._validate_target_column_data_type`, preserving the existing messages. In the validator, catch the domain exception from the parser, add the original configured value to the message, and chain with `from error`; for example:

```python
except InvalidTargetColumnDataTypeError as error:
    raise InvalidTargetColumnDataTypeError(
        f"Invalid target_column_data_type {value!r}: {error}"
    ) from error
```

If you reused an existing error, substitute its name in that snippet. A `ValueError` caught from `TargetType(normalized)` is normal enum lookup control flow; leave that catch alone. Make sure any failure from the parser's `int(...)` conversions also becomes the shared-library domain exception (a very long digit string can make `int` raise `ValueError`).

In `src/shared_lib/pipelines/data_vault_pipeline.py`, continue translating parser failure into the existing contextual `SchemaBuilderError` with column and table names and exception chaining. Catch the domain exception specifically once the parser consistently raises it. Never raise a plain custom `Exception`, `ConfigurationError`, or `SchemaBuilderError` **inside** the Pydantic field validator unless you have verified it inherits `ValueError`: Pydantic v2 converts `ValueError` subclasses into `ValidationError`, whereas arbitrary exceptions escape directly. Keep tests asserting `ValidationError` for invalid `ColumnConfig` input and `SchemaBuilderError` for malformed decimal passed directly to the schema resolver.

Use a **single** appropriate error class, not a new exception for each invalid input or each layer. Avoid catching and replacing unrelated programming errors. Check one direct `parse_decimal_parameters` rejection raises this shared-library error, while constructing an invalid `ColumnConfig` still raises Pydantic `ValidationError` (not that raw error).

## 2. Check the regex against its intended grammar

Inspect `_DECIMAL_RE` and `parse_decimal_parameters` in `src/shared_lib/layers/shared/config.py`. The existing `fullmatch` pattern using `[ \t]*` and `[0-9]+` is a valid, small grammar for `decimal(p,s)`: it allows spaces/tabs within the expression, accepts ASCII digits, and rejects trailing text and embedded newlines. Keep it unless you find a demonstrable bug. In particular, do **not** replace `[ \t]` with `\s` (that would also accept newlines), `[0-9]` with `\d` (that also accepts Unicode digits), or `fullmatch` with a prefix match. Do not add a second parser or a manual `split`/`strip` implementation solely because the regex looks dense. A short explanatory comment about horizontal whitespace is fine if it genuinely improves readability.

Confirm the following with existing tests; add only missing representative cases:

| Input | Expected |
| --- | --- |
| `decimal(10, 4)`, ` DECIMAL ( 10 , 4 ) ` | Canonical `decimal(10,4)` |
| `decimal\t(\t10\t,\t4\t)` | Canonical `decimal(10,4)` |
| `decimal(10,\n4)`, `decimal(10,4)extra`, `decimal(10,4,2)` | Validation error |
| `decimal(0,0)`, `decimal(39,0)`, `decimal(10,11)` | Validation error |
| bare `decimal` | Existing enum and `DecimalType(10, 0)` |

## 3. Check the `TargetType | str` contract

Keep `target_column_data_type: TargetType | str` for this focused change. The annotation describes the **stored** values; it does not grant arbitrary strings to valid configurations. The `mode="before"` validator must turn known plain types and bare `decimal` into `TargetType`, turn only valid `decimal(p,s)` into a canonical string, and reject every other string. Confirm those three paths in tests, including `"uuid"` and `model_dump(mode="json")` for both representations. Search for `model_copy(update=...)`, `model_construct`, or direct assignment of this field: these can bypass creation-time validation and must not inject unvalidated strings. Do not change the public field into a dataclass merely to narrow the annotation during final polish; that changes consumers and the default serialized shape of parameterized decimals. If multiple consumers genuinely need structured precision/scale instead of the canonical string, flag a separate typed-value design with an explicit serialization contract.

## 4. Clean up the changed tests, especially comments

Review all tests touched by this feature, including `test_config_unit.py`, `test_config_loader_unit.py`, and `test_data_vault_pipeline_unit.py`. For new or changed tests with separate setup/action/assertion phases, use exactly the surrounding lowercase, standalone `# given`, `# when`, `# then` markers in that order. Don't use `# then - ...` or other improvised headings. Each comment must accurately describe the following step. In `test_decimal_with_custom_precision_and_scale`, remove or correct `# then - nondecimal enum, explicit decimal, bare decimal`: the three `_mock_column` inputs are **strings** (`decimal(14,2)`, `decimal(7,4)`, `decimal`), all decimal types, so that claim is false. Replace the YAML test's `# then - quoted YAML decimal reaches ColumnConfig as canonical string` with a plain `# then` above the assertions. For concise single-assertion tests, introduce local variables to make `when` and `then` clear where useful; do not invent dummy setup solely to place a `# given` comment. Keep fixtures/parameterization meaningful, and do not rewrite unrelated existing tests.

Verify that at least one schema-building test passes **real, validated `ColumnConfig` values** through `_build_schema_from_config` to exercise an ordinary `TargetType` enum, bare decimal, and explicit decimal. A `MagicMock` configured only with strings does not prove enum integration. If an existing test covers that path, leave it alone; otherwise add one focused test. Check the YAML loading test keeps its quoted `decimal(10, 4)` and asserts the canonical value.

## Finish

Search for other consumers of `target_column_data_type` that still assume every value has `.value`, especially conversion/cast paths. Run the project's relevant config, config-loader, entity, and pipeline tests, its configured lint/format checks, and `git diff --check`. Inspect the final diff for scope, imports, exception behavior, and truthful test names/comments. Report exactly which files changed, what ran, and any checks blocked by the environment. Keep this follow-up limited to issues actually found.
