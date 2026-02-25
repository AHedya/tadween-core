from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import fields, is_dataclass
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T")


class SchemaAdapter(ABC):
    """
    Abstract adapter for different schema types (dataclass, Pydantic, etc.).
    Provides a unified interface for schema introspection and manipulation.
    """

    def __init__(self, schema_type: type) -> None:
        self.schema_type = schema_type

    @abstractmethod
    def get_field_names(self) -> list[str]:
        """Return list of field names in the schema."""
        pass

    @abstractmethod
    def extract_values(self, instance: Any) -> dict[str, Any]:
        """Extract field values from a schema instance as a dictionary."""
        pass

    @abstractmethod
    def create_instance(self, values: dict[str, Any]) -> Any:
        """Create a new schema instance from field values."""
        pass

    @abstractmethod
    def validate_field(self, field_name: str, value: Any) -> Any:
        """
        Validate and potentially coerce a field value.
        Returns the validated value, or raises an exception if invalid.
        """
        pass


class DataclassAdapter(SchemaAdapter):
    """Adapter for dataclass schemas."""

    def __init__(self, schema_type: type) -> None:
        if not is_dataclass(schema_type):
            raise TypeError(f"{schema_type.__name__} is not a dataclass")
        super().__init__(schema_type)
        self._field_names = [f.name for f in fields(schema_type)]

    def get_field_names(self) -> list[str]:
        return self._field_names

    def extract_values(self, instance: Any) -> dict[str, Any]:
        return {name: getattr(instance, name) for name in self._field_names}

    def create_instance(self, values: dict[str, Any]) -> Any:
        return self.schema_type(**values)

    def validate_field(self, field_name: str, value: Any) -> Any:
        # Dataclasses don't have built-in runtime validation
        # Could add type checking here if needed
        return value


class PydanticAdapter(SchemaAdapter):
    def __init__(self, schema_type: type) -> None:
        if not issubclass(schema_type, BaseModel):
            raise TypeError(f"{schema_type.__name__} is not a Pydantic BaseModel")
        self.schema_type = schema_type
        self._field_names = list(schema_type.model_fields.keys())

    def get_field_names(self) -> list[str]:
        return self._field_names

    def extract_values(self, instance: Any) -> dict[str, Any]:
        return instance.model_dump()

    def create_instance(self, values: dict[str, Any]) -> Any:
        return self.schema_type(**values)

    def validate_field(self, field_name: str, value: Any) -> Any:
        """Use Pydantic's field validators for single field validation."""
        try:
            self.schema_type.__pydantic_validator__.validate_assignment(
                self.schema_type.model_construct(), field_name, value
            )
            return value
        except ValidationError as e:
            raise ValueError(f"Validation failed for field '{field_name}': {e}") from e


def create_schema_adapter(schema_type: type) -> SchemaAdapter:
    """
    Factory function to create the appropriate adapter for a schema type.
    Args:
        schema_type: A dataclass or Pydantic BaseModel class
    Returns:
        Appropriate SchemaAdapter instance
    Raises:
        TypeError: If schema_type is neither a dataclass nor Pydantic model
    """
    if is_dataclass(schema_type):
        return DataclassAdapter(schema_type)
    elif issubclass(schema_type, BaseModel):
        return PydanticAdapter(schema_type)
    else:
        raise TypeError(
            f"Unsupported schema type: {schema_type.__name__}. "
            "Must be a dataclass or Pydantic BaseModel."
        )
