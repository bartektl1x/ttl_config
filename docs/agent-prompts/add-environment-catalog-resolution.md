# Agent task: add environment-aware catalog resolution to ttl_config_v2

## Goal

Apply one minimal feature to the current `ttl_config_v2` implementation:

- The Excel workbook may contain either:
  - an environment-neutral catalog, for example `sales`, or
  - the canonical production-prefixed catalog, for example `prd_sales`.
- The Databricks job will pass the current deployment environment to `RetentionConfigGenerator.generate_retention_config(...)`.
- Allowed environments are exactly:
  - `dev`
  - `qat`
  - `uat`
  - `prp`
  - `prd`
- Before inheritance and Unity Catalog validation, the generator must resolve the catalog to the physical catalog for the current environment.
- Do not redesign any other class.

Examples:

```text
Excel catalog    environment    resolved catalog
sales            dev            dev_sales
prd_sales        dev            dev_sales
sales            qat            qat_sales
prd_sales        qat            qat_sales
sales            uat            uat_sales
prd_sales        prp            prp_sales
prd_sales        prd            prd_sales
```

The Databricks job is expected to call the generator like this:

```python
rules = generator.generate_retention_config(
    workbook_path=workbook_path,
    sheet_name="Retention",
    environment=spark.conf.get("env"),
    include_inheritance=True,
)
```

The generator itself must not read `spark.conf["env"]`. It should receive `environment` explicitly.

---

## Scope

Modify only:

```text
ttl_config_v2/retention_config/retention_config_generator.py
ttl_config_v2/tests/unit/test_retention_config_generator_unit.py
```

Do not modify:

```text
retention_rules.py
excel_retention_rules.py
retention_target_validator.py
retention_rule_inheritance.py
integration tests
Excel fixtures
README
```

Do not introduce new classes, managers, resolvers, factories, config objects, enums, or generic abstractions.

This is intentionally one small orchestration concern owned by `RetentionConfigGenerator`.

---

# 1. Production change

File:

```text
ttl_config_v2/retention_config/retention_config_generator.py
```

## 1.1 Import `RetentionRule`

Change the existing import:

```python
from retention_config.retention_rules import (
    RetentionRules,
    TableName,
)
```

to:

```python
from retention_config.retention_rules import (
    RetentionRule,
    RetentionRules,
    TableName,
)
```

Do not change the package namespace in this task.

---

## 1.2 Add constants to `RetentionConfigGenerator`

Directly below the existing destination constants:

```python
_OPS_CATALOG_CONFIG = "ops_catalog"
_RETENTION_SCHEMA = "retention"
_RETENTION_TABLE = "retention_config"
```

add:

```python
_ALLOWED_ENVIRONMENTS = (
    "dev",
    "qat",
    "uat",
    "prp",
    "prd",
)
_CANONICAL_CATALOG_PREFIX = "prd_"
```

Keep them as class constants.

Do not introduce an Enum.

---

## 1.3 Change `generate_retention_config`

Add a required keyword-only `environment: str` argument.

The signature must become:

```python
def generate_retention_config(
    self,
    workbook_path: str,
    sheet_name: str,
    *,
    environment: str,
    include_inheritance: bool = False,
) -> RetentionRules:
```

Immediately after reading the Excel rules, resolve catalogs:

```python
rules = self._read_retention_rules(
    workbook_path,
    sheet_name,
)

rules = self._resolve_environment_catalogs(
    rules,
    environment,
)

if include_inheritance:
    rules = self._inherit_retention_rules(rules)

self._validate_retention_targets(rules)

return rules
```

The order is important and must remain:

```text
Excel read
-> environment catalog resolution
-> optional inheritance
-> Unity Catalog validation
-> return effective rules
```

Why:

- Excel represents canonical/logical configuration.
- Lineage system tables contain physical environment-specific catalog names.
- Unity Catalog validation must validate physical environment-specific targets.

Do not resolve catalogs after inheritance or validation.

---

## 1.4 Add `_resolve_environment_catalogs`

Physically place this method immediately after `_read_retention_rules` and before `_inherit_retention_rules`, so the file continues to read top-down in call order.

Add exactly this behavior:

```python
def _resolve_environment_catalogs(
    self,
    rules: RetentionRules,
    environment: str,
) -> RetentionRules:
    environment = environment.strip().lower()

    if environment not in self._ALLOWED_ENVIRONMENTS:
        raise ValueError(
            "environment must be one of "
            + ", ".join(self._ALLOWED_ENVIRONMENTS)
        )

    resolved_rules: list[RetentionRule] = []
    resolved_tables: set[TableName] = set()

    for rule in rules:
        table_name = TableName(
            catalog=self._resolve_catalog(
                rule.table_name.catalog,
                environment,
            ),
            schema=rule.table_name.schema_name,
            table=rule.table_name.table,
        )

        if table_name in resolved_tables:
            raise ValueError(
                "Environment catalog resolution produced duplicate "
                f"retention target {table_name.sql_identifier}"
            )

        resolved_tables.add(table_name)

        resolved_rules.append(
            RetentionRule(
                table_name=table_name,
                time_column=rule.time_column,
                expiration_days=rule.expiration_days,
            )
        )

    return tuple(resolved_rules)
```

Do not use `model_copy(update=...)`.

Reconstruct `TableName` and `RetentionRule` normally so Pydantic validation remains active.

The duplicate check is required because these two distinct Excel rows:

```text
sales     retention.orders
prd_sales retention.orders
```

would both resolve to:

```text
dev_sales.retention.orders
```

for `environment="dev"`.

That collision must fail before inheritance or target validation.

---

## 1.5 Add `_resolve_catalog`

Place this directly after `_resolve_environment_catalogs`.

Add:

```python
@classmethod
def _resolve_catalog(
    cls,
    catalog: str,
    environment: str,
) -> str:
    environment_prefixes = tuple(
        f"{candidate}_"
        for candidate in cls._ALLOWED_ENVIRONMENTS
    )

    existing_prefix = next(
        (
            prefix
            for prefix in environment_prefixes
            if catalog.startswith(prefix)
        ),
        None,
    )

    if (
        existing_prefix is not None
        and existing_prefix != cls._CANONICAL_CATALOG_PREFIX
    ):
        raise ValueError(
            "Excel catalog must be environment-neutral or start with "
            f"{cls._CANONICAL_CATALOG_PREFIX!r}; found {catalog!r}"
        )

    canonical_catalog = catalog.removeprefix(
        cls._CANONICAL_CATALOG_PREFIX
    )

    return f"{environment}_{canonical_catalog}"
```

The behavior must be:

```text
sales       + dev -> dev_sales
prd_sales   + dev -> dev_sales
sales       + qat -> qat_sales
prd_sales   + uat -> uat_sales
prd_sales   + prp -> prp_sales
prd_sales   + prd -> prd_sales
```

Environment-specific Excel catalogs other than `prd_` must be rejected:

```text
dev_sales
qat_sales
uat_sales
prp_sales
```

Reason: the workbook contract is canonical/environment-neutral or `prd_` canonical input only. We must not silently reinterpret configuration already tied to another deployment environment.

Do not use generic string replacement such as:

```python
catalog.replace("prd", environment)
catalog.replace("prd_", f"{environment}_")
```

Use `removeprefix` as shown above.

---

# 2. Unit test changes

File:

```text
ttl_config_v2/tests/unit/test_retention_config_generator_unit.py
```

No integration test is required for this feature because catalog resolution is deterministic Python/domain orchestration and does not require Spark execution.

---

## 2.1 Update test helpers to accept catalog

Replace:

```python
def _table(name: str) -> TableName:
    return TableName(
        catalog="main",
        schema="retention",
        table=name,
    )
```

with:

```python
def _table(
    name: str,
    *,
    catalog: str = "main",
) -> TableName:
    return TableName(
        catalog=catalog,
        schema="retention",
        table=name,
    )
```

Replace:

```python
def _rule(
    table: str,
    expiration_days: int = 30,
) -> RetentionRule:
    return RetentionRule(
        table_name=_table(table),
        time_column="event_time",
        expiration_days=expiration_days,
    )
```

with:

```python
def _rule(
    table: str,
    expiration_days: int = 30,
    *,
    catalog: str = "main",
) -> RetentionRule:
    return RetentionRule(
        table_name=_table(
            table,
            catalog=catalog,
        ),
        time_column="event_time",
        expiration_days=expiration_days,
    )
```

---

## 2.2 Update explicit-only orchestration test

The test must prove:

```text
read -> resolve -> validate
```

and that inheritance is not used.

Use this shape:

```python
def test_generate_retention_config_without_inheritance_validates_resolved_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    generator, _ = _generator()
    explicit_rules = (_rule("orders"),)
    resolved_rules = (_rule("orders", catalog="dev_main"),)

    read_rules = MagicMock(return_value=explicit_rules)
    resolve_catalogs = MagicMock(return_value=resolved_rules)
    inherit_rules = MagicMock(
        side_effect=AssertionError("Inheritance must not be used"),
    )
    validate_targets = MagicMock()

    monkeypatch.setattr(generator, "_read_retention_rules", read_rules)
    monkeypatch.setattr(
        generator,
        "_resolve_environment_catalogs",
        resolve_catalogs,
    )
    monkeypatch.setattr(generator, "_inherit_retention_rules", inherit_rules)
    monkeypatch.setattr(
        generator,
        "_validate_retention_targets",
        validate_targets,
    )

    # when
    rules = generator.generate_retention_config(
        "retention.xlsx",
        "Retention",
        environment="dev",
    )

    # then
    assert rules == resolved_rules
    read_rules.assert_called_once_with(
        "retention.xlsx",
        "Retention",
    )
    resolve_catalogs.assert_called_once_with(
        explicit_rules,
        "dev",
    )
    inherit_rules.assert_not_called()
    validate_targets.assert_called_once_with(resolved_rules)
```

---

## 2.3 Update inheritance orchestration test

The test must prove:

```text
read -> resolve -> inherit -> validate
```

Use:

```python
def test_generate_retention_config_with_inheritance_validates_effective_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # given
    generator, _ = _generator()
    explicit_rules = (_rule("orders"),)
    resolved_rules = (_rule("orders", catalog="dev_main"),)
    effective_rules = (
        *resolved_rules,
        _rule("customers", catalog="dev_main"),
    )

    read_rules = MagicMock(return_value=explicit_rules)
    resolve_catalogs = MagicMock(return_value=resolved_rules)
    inherit_rules = MagicMock(return_value=effective_rules)
    validate_targets = MagicMock()

    monkeypatch.setattr(generator, "_read_retention_rules", read_rules)
    monkeypatch.setattr(
        generator,
        "_resolve_environment_catalogs",
        resolve_catalogs,
    )
    monkeypatch.setattr(generator, "_inherit_retention_rules", inherit_rules)
    monkeypatch.setattr(
        generator,
        "_validate_retention_targets",
        validate_targets,
    )

    # when
    rules = generator.generate_retention_config(
        "retention.xlsx",
        "Retention",
        environment="dev",
        include_inheritance=True,
    )

    # then
    assert rules == effective_rules
    resolve_catalogs.assert_called_once_with(
        explicit_rules,
        "dev",
    )
    inherit_rules.assert_called_once_with(resolved_rules)
    validate_targets.assert_called_once_with(effective_rules)
```

---

## 2.4 Add catalog-resolution behavior test

Add:

```python
@pytest.mark.parametrize(
    ("catalog", "environment", "expected_catalog"),
    [
        ("sales", "dev", "dev_sales"),
        ("prd_sales", "dev", "dev_sales"),
        ("sales", "qat", "qat_sales"),
        ("prd_sales", "qat", "qat_sales"),
        ("sales", "uat", "uat_sales"),
        ("prd_sales", "prp", "prp_sales"),
        ("prd_sales", "prd", "prd_sales"),
        ("prd_sales", " DEV ", "dev_sales"),
    ],
)
def test_resolve_environment_catalogs(
    catalog: str,
    environment: str,
    expected_catalog: str,
) -> None:
    # given
    generator, _ = _generator()
    rules = (
        _rule(
            "orders",
            catalog=catalog,
        ),
    )

    # when
    resolved_rules = generator._resolve_environment_catalogs(
        rules,
        environment,
    )

    # then
    assert resolved_rules == (
        _rule(
            "orders",
            catalog=expected_catalog,
        ),
    )
```

This test intentionally also proves environment normalization through `" DEV "`.

---

## 2.5 Add unsupported environment test

Add:

```python
@pytest.mark.parametrize(
    "environment",
    [
        "",
        "sit",
        "prod",
    ],
)
def test_resolve_environment_catalogs_rejects_unsupported_environment(
    environment: str,
) -> None:
    # given
    generator, _ = _generator()

    # when / then
    with pytest.raises(
        ValueError,
        match="environment must be one of",
    ):
        generator._resolve_environment_catalogs(
            (_rule("orders", catalog="sales"),),
            environment,
        )
```

Do not accept aliases such as:

```text
prod
production
preprod
pre-production
```

Only the five explicitly supported values are valid.

---

## 2.6 Add environment-specific Excel input rejection test

Add:

```python
@pytest.mark.parametrize(
    "catalog",
    [
        "dev_sales",
        "qat_sales",
        "uat_sales",
        "prp_sales",
    ],
)
def test_resolve_environment_catalogs_rejects_environment_specific_input(
    catalog: str,
) -> None:
    # given
    generator, _ = _generator()

    # when / then
    with pytest.raises(
        ValueError,
        match="environment-neutral or start with 'prd_'",
    ):
        generator._resolve_environment_catalogs(
            (_rule("orders", catalog=catalog),),
            "dev",
        )
```

`prd_sales` must not be included here because it is explicitly valid canonical input.

---

## 2.7 Add post-resolution duplicate test

Add:

```python
def test_resolve_environment_catalogs_rejects_duplicate_resolved_target() -> None:
    # given
    generator, _ = _generator()
    rules = (
        _rule(
            "orders",
            catalog="sales",
        ),
        _rule(
            "orders",
            catalog="prd_sales",
        ),
    )

    # when / then
    with pytest.raises(
        ValueError,
        match="Environment catalog resolution produced duplicate retention target",
    ):
        generator._resolve_environment_catalogs(
            rules,
            "dev",
        )
```

This is an important regression test for the new feature.

Do not move this validation into the Excel reader. Before environment resolution the two targets are different and the Excel reader is correct to accept them.

---

# 3. Existing tests outside these changes

Keep all existing writer tests unchanged unless a helper signature update requires a trivial call-site adjustment.

Do not add environment logic to:

- `write_retention_config`
- destination catalog resolution
- `_resolve_retention_config_table`

The destination configuration table still uses:

```python
spark.conf.get("ops_catalog")
```

This feature applies only to retention target catalogs read from Excel.

Do not modify integration tests for this feature.

---

# 4. Required final method order

Within `RetentionConfigGenerator`, preserve this conceptual order:

```text
generate_retention_config
    _read_retention_rules
    _resolve_environment_catalogs
        _resolve_catalog
    _inherit_retention_rules
    _validate_retention_targets

write_retention_config
    _resolve_retention_config_table
    _validate_retention_config_table
    ...
    _prepare_retention_config
```

Do not reorganize unrelated methods.

---

# 5. Acceptance criteria

The change is complete only if all of the following are true:

1. `generate_retention_config` has required keyword-only `environment: str`.
2. Allowed values are exactly `dev`, `qat`, `uat`, `prp`, `prd`.
3. Environment input is normalized with `strip().lower()`.
4. Unprefixed Excel catalog names receive the environment prefix.
5. `prd_` Excel catalog names have only that prefix replaced with the target environment prefix.
6. `dev_`, `qat_`, `uat_`, and `prp_` input catalogs are rejected.
7. Generic `.replace(...)` is not used for prefix conversion.
8. Catalog resolution happens before inheritance.
9. Catalog resolution happens before Unity Catalog target validation.
10. Two canonical inputs that resolve to the same physical target raise `ValueError`.
11. No production class other than `RetentionConfigGenerator` is changed.
12. No integration test is added just for this pure deterministic transformation.
13. Existing retention behavior remains unchanged outside catalog environment resolution.
14. All existing unit tests continue to pass after updating the two generator orchestration tests.
15. New unit tests cover mapping, invalid environments, invalid environment-specific input, and post-resolution duplicate targets.

---

# 6. Important non-goals

Do not:

- create a separate environment resolver class,
- introduce an enum,
- read the environment from Spark inside the generator,
- add environment awareness to `TableName`,
- add environment awareness to `ExcelRetentionRulesReader`,
- add environment awareness to `RetentionTargetValidator`,
- add environment awareness to `RetentionRuleInheritanceResolver`,
- change output-table destination handling,
- add fallback aliases such as `prod -> prd`,
- support arbitrary prefixes,
- redesign the existing retention architecture,
- change package names,
- move files as part of this task,
- fix unrelated PR-review findings.

This task should be a small, focused patch.

---

# 7. Verification before committing implementation

After applying the change:

1. Show the diff for exactly the two modified files.
2. Confirm no unrelated files changed.
3. Run syntax compilation for both modified Python files.
4. Run the generator unit test file if the environment has pytest + pyspark available.
5. If tests cannot run because of missing dependencies, say so explicitly; do not claim they passed.
6. Report the exact behavior of:
   - `sales + dev`
   - `prd_sales + dev`
   - `prd_sales + prd`
   - `dev_sales + dev`
   - invalid environment `prod`
   - duplicate `sales/orders` + `prd_sales/orders` under `dev`.
7. Commit only the implementation/test patch with a focused commit message such as:

```text
feat: resolve retention catalogs by environment
```

Do not include unrelated cleanup or refactoring in that commit.
