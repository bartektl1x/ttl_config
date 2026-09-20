"""Retention configuration for Unity Catalog tables."""

from ttl_config.retention_rules import (
    MAX_EXPIRATION_DAYS,
    RetentionRule,
    RetentionRules,
    SchemaName,
    TableName,
)

__all__ = [
    "MAX_EXPIRATION_DAYS",
    "RetentionRule",
    "RetentionRules",
    "SchemaName",
    "TableName",
]
