from typing import Any

from .cache import BucketSchemaT, Cache
from .policy import CachePolicy

_cache_singleton: Cache[Any] | None = None
_cache_schema_type: type[Any] | None = None


def get_cache(
    schema_type: type[BucketSchemaT] | None = None,
    *,
    policy: CachePolicy | None = None,
) -> "Cache[BucketSchemaT]":
    global _cache_singleton, _cache_schema_type

    if _cache_singleton is None:
        if schema_type is None:
            raise RuntimeError(
                "Cache is not initialized yet. "
                "You must provide schema_type on first call."
            )

        _cache_singleton = Cache(schema_type, policy=policy)
        _cache_schema_type = schema_type
        return _cache_singleton

    if schema_type is not None and schema_type is not _cache_schema_type:
        raise TypeError(
            f"Cache already initialized with schema "
            f"{_cache_schema_type.__name__}, "
            f"cannot reinitialize with {schema_type.__name__}"
        )

    return _cache_singleton


__all__ = ["Cache", "get_cache"]
