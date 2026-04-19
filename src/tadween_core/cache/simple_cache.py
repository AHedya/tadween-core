import threading
from typing import TypeVar

from tadween_core.cache.base import BaseCache

BucketSchemaT = TypeVar("BucketSchemaT")


class SimpleCache(BaseCache[BucketSchemaT]):
    """Type-safe, thread-safe alternative to `Cache`.

    All public methods are thread-safe as they are just internal dict wrappers. However,
    for read-write steps its recommended to use SimpleCache.lock to ensure true thread-safety
    especially in thread-free builds.

    Example:
    ```python
    with SimpleCache.lock:
        if "key" in SimpleCache:
            SimpleCache['key'] = "new_value"
    ```
    """

    def __init__(self, schema_type: type[BucketSchemaT]) -> None:
        self._schema_type = schema_type
        self._store: dict[str, BucketSchemaT] = {}
        self._lock = threading.RLock()

    @property
    def schema_type(self) -> type[BucketSchemaT]:
        return self._schema_type

    @property
    def lock(self):
        return self._lock

    def get_bucket(self, key: str) -> BucketSchemaT | None:
        """Retrieve a cached instance by key. Returns None if not found."""
        return self[key]

    def set_bucket(self, key: str, bucket: BucketSchemaT) -> bool:
        """Store an instance under key. Overwrites if exists.

        Returns:
            True if successful.

        Raises:
            TypeError: if bucket doesn't match specified `schema_type`
        """
        self[key] = bucket
        return True

    def delete_bucket(self, key: str):
        """Remove a key from the cache. No-op if key does not exist."""
        del self[key]

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._store.clear()

    def keys(self) -> list[str]:
        return list(self._store.keys())

    def __getitem__(self, key: str) -> BucketSchemaT | None:
        return self._store.get(key)

    def __setitem__(self, key: str, bucket: BucketSchemaT) -> None:
        if not isinstance(bucket, self._schema_type):
            raise TypeError(
                f"Expected {self._schema_type.__name__}, got {type(bucket).__name__}"
            )
        self._store[key] = bucket

    def __delitem__(self, key: str) -> None:
        self._store.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)
