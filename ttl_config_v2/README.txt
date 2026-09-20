Retention configuration complete package

Contents:
- retention_config/: production implementation modules
- tests/unit/: locked unit test suites
- tests/integration/: locked integration test suites
- tests/resources/retention_rules.xlsx: integration Excel fixture
- manual_test_retention_rules.xlsx: manual test workbook

Notes:
- Integration tests require the expected Databricks/Spark environment and permissions.
- The package uses Python 3.12 type-alias syntax.
