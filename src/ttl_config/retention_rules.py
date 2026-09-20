from typing import Annotated, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)


MAX_EXPIRATION_DAYS = 2**63 - 1


type NonEmptyString = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
    ),
]

type UnityCatalogNameComponent = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        to_lower=True,
        min_length=1,
        max_length=255,
    ),
]


class SchemaName(BaseModel):
    """Canonical Unity Catalog schema identity."""

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
    def _validate_name_components(self) -> Self:
        self._validate_no_periods()
        self._validate_no_internal_spaces()
        self._validate_no_slashes()
        self._validate_no_control_characters()

        return self

    def _validate_no_periods(self) -> None:
        if any("." in component for component in self._components):
            raise ValueError(
                "Unity Catalog name components cannot contain periods"
            )

    def _validate_no_internal_spaces(self) -> None:
        if any(" " in component for component in self._components):
            raise ValueError(
                "Unity Catalog name components cannot contain spaces"
            )

    def _validate_no_slashes(self) -> None:
        if any("/" in component for component in self._components):
            raise ValueError(
                "Unity Catalog name components cannot contain slashes"
            )

    def _validate_no_control_characters(self) -> None:
        if any(
            ord(character) < 32 or ord(character) == 127
            for component in self._components
            for character in component
        ):
            raise ValueError(
                "Unity Catalog name components cannot contain control characters"
            )

    @property
    def sql_identifier(self) -> str:
        return ".".join(
            f"`{component.replace('`', '``')}`"
            for component in self._components
        )

    @property
    def _components(self) -> tuple[str, ...]:
        return (
            self.catalog,
            self.schema_name,
        )


class TableName(SchemaName):
    """Canonical Unity Catalog table identity."""

    table: UnityCatalogNameComponent

    @property
    def _components(self) -> tuple[str, ...]:
        return (
            self.catalog,
            self.schema_name,
            self.table,
        )


class RetentionRule(BaseModel):
    """Retention policy for one Unity Catalog table."""

    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        validate_by_name=True,
        validate_by_alias=True,
    )

    table_name: TableName
    time_column: NonEmptyString
    expiration_days: int = Field(
        ge=0,
        le=MAX_EXPIRATION_DAYS,
    )

    @field_validator("time_column")
    @classmethod
    def _validate_time_column(
        cls,
        value: str,
    ) -> str:
        if "." in value:
            raise ValueError(
                "time_column must be a top-level column name without periods; "
                "nested field paths are not supported"
            )

        if any(
            ord(character) < 32 or ord(character) == 127
            for character in value
        ):
            raise ValueError(
                "time_column cannot contain control characters"
            )

        return value


type RetentionRules = tuple[RetentionRule, ...]
