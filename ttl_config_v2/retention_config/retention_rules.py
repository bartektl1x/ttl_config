from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator

MAX_EXPIRATION_DAYS = 2**63 - 1

NonEmptyString = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
    ),
]
UnityCatalogNameComponent = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        to_lower=True,
        min_length=1,
        max_length=255,
    ),
]


class SchemaName(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        validate_by_name=True,
        validate_by_alias=True,
    )

    catalog: UnityCatalogNameComponent
    schema_name: UnityCatalogNameComponent = Field(alias="schema")

    @model_validator(mode="after")
    def _validate_name_components(self) -> SchemaName:
        self._validate_no_periods()
        self._validate_no_internal_spaces()
        self._validate_no_slashes()
        self._validate_no_control_characters()
        return self

    @property
    def sql_identifier(self) -> str:
        return ".".join(
            f"`{component.replace('`', '``')}`"
            for component in self._components()
        )

    def _components(self) -> tuple[str, ...]:
        return self.catalog, self.schema_name

    def _validate_no_periods(self) -> None:
        if any("." in component for component in self._components()):
            raise ValueError("Unity Catalog name components must not contain periods")

    def _validate_no_internal_spaces(self) -> None:
        if any(any(character.isspace() for character in component) for component in self._components()):
            raise ValueError("Unity Catalog name components must not contain whitespace")

    def _validate_no_slashes(self) -> None:
        if any("/" in component or "\\" in component for component in self._components()):
            raise ValueError("Unity Catalog name components must not contain slashes")

    def _validate_no_control_characters(self) -> None:
        if any(
            any(ord(character) < 32 or ord(character) == 127 for character in component)
            for component in self._components()
        ):
            raise ValueError("Unity Catalog name components must not contain control characters")


class TableName(SchemaName):
    table: UnityCatalogNameComponent

    def _components(self) -> tuple[str, ...]:
        return self.catalog, self.schema_name, self.table


class RetentionRule(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        validate_by_name=True,
        validate_by_alias=True,
    )

    table_name: TableName
    time_column: NonEmptyString
    expiration_days: int = Field(ge=0, le=MAX_EXPIRATION_DAYS)

    @field_validator("time_column")
    @classmethod
    def _validate_time_column(cls, time_column: str) -> str:
        if "." in time_column:
            raise ValueError("time_column must be a top-level column name")

        if any(ord(character) < 32 or ord(character) == 127 for character in time_column):
            raise ValueError("time_column must not contain control characters")

        return time_column


type RetentionRules = tuple[RetentionRule, ...]
