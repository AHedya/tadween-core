from collections.abc import Callable
from typing import Any, Generic, TypeVar

from tadween_core.cache.adapter import SchemaAdapter
from tadween_core.cache.base import CacheEntry

T = TypeVar("T")


class BucketProxy(Generic[T]):
    """
    Runtime proxy that exposes schema fields with their original types while
    internally delegating to CacheEntry objects for storage and metadata tracking.

    This enables transparent caching with automatic touch() on reads and
    metadata updates on writes, while maintaining full type hints for IDEs.

    `_on_read` and `_on_write` are hooks passed by the `Cache` object to update the `BucketProxy` instance on read/write
    """

    __slots__ = ("_internal", "_adapter", "_on_read", "_on_write")

    def __init__(
        self,
        internal_map: dict[str, CacheEntry[Any]],
        adapter: SchemaAdapter,
        on_read: Callable[[str, CacheEntry[Any]], Any] | None = None,
        on_write: Callable[[str, Any, int | None, int | None], None] | None = None,
    ) -> None:
        object.__setattr__(self, "_internal", internal_map)
        object.__setattr__(self, "_adapter", adapter)
        object.__setattr__(self, "_on_read", on_read)
        object.__setattr__(self, "_on_write", on_write)

    def __getattr__(self, name: str) -> Any:
        internal: dict[str, CacheEntry[Any]] = object.__getattribute__(
            self, "_internal"
        )

        if name in internal:
            entry = internal[name]
            entry.touch()

            on_read = object.__getattribute__(self, "_on_read")
            if on_read:
                return on_read(name, entry)

            return entry.value

        adapter: SchemaAdapter = object.__getattribute__(self, "_adapter")
        raise AttributeError(
            f"'{name}' is not a field in {adapter.schema_type.__name__}.\n"
            f"Available fields: {', '.join(internal.keys())}\n\n"
            f"BucketProxy only exposes data fields (getters/setters), not methods or validators.\n"
            f"If you need to call methods or use isinstance(), convert to a real instance with .to_instance()"
        )

    def __setattr__(self, name: str, value: Any) -> None:
        self.set_entry(name, value)

    def set_entry(self, name: str, value: Any, quota: int | None = None) -> None:
        """
        Update a field with an optional per-entry read quota.

        Args:
            name: Field name
            value: New value
            quota: Number of reads before eviction (overrides policy)
        """
        internal: dict[str, CacheEntry[Any]] = object.__getattribute__(
            self, "_internal"
        )
        adapter: SchemaAdapter = object.__getattribute__(self, "_adapter")

        if name in internal:
            validated_value = adapter.validate_field(name, value)

            on_write = object.__getattribute__(self, "_on_write")

            if on_write:
                on_write(field_name=name, value=validated_value, quota=quota)
            return

        raise AttributeError(
            f"'{adapter.schema_type.__name__}' has no attribute '{name}'"
        )

    def to_instance(self) -> T:
        """
        Create a fresh schema instance with current cached values.
        Returns:
            New instance of the schema type (dataclass or Pydantic model)
        """
        adapter: SchemaAdapter = object.__getattribute__(self, "_adapter")
        internal: dict[str, CacheEntry[Any]] = object.__getattribute__(
            self, "_internal"
        )
        values = {name: entry.value for name, entry in internal.items()}
        return adapter.create_instance(values)

    def get_entry(self, field_name: str) -> CacheEntry[Any]:
        """
        Access the underlying CacheEntry for a field.
        Useful for inspecting metadata like read_count, size, and created_at.
        Args:
            field_name: Name of the field
        Returns:
            CacheEntry object for the field
        Raises:
            AttributeError: If field doesn't exist
        """
        internal: dict[str, CacheEntry[Any]] = object.__getattribute__(
            self, "_internal"
        )
        adapter: SchemaAdapter = object.__getattribute__(self, "_adapter")

        if field_name not in internal:
            raise AttributeError(
                f"'{adapter.schema_type.__name__}' has no attribute '{field_name}'"
            )
        return internal[field_name]

    def __repr__(self) -> str:
        adapter: SchemaAdapter = object.__getattribute__(self, "_adapter")
        internal: dict[str, CacheEntry[Any]] = object.__getattribute__(
            self, "_internal"
        )
        values = {name: entry.value for name, entry in internal.items()}
        return f"<BucketProxy[{adapter.schema_type.__name__}]({values})>"

    def __dir__(self) -> list[str]:
        internal: dict[str, CacheEntry[Any]] = object.__getattribute__(
            self, "_internal"
        )
        return sorted(list(internal.keys()) + dir(self.__class__))
