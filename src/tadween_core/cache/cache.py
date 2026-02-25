import sys
import time
from typing import Any, Generic, TypeVar

from tadween_core.cache.adapter import create_schema_adapter
from tadween_core.cache.base import CacheEntry
from tadween_core.cache.proxy import BucketProxy

from .policy import CachePolicy

BucketSchemaT = TypeVar("BucketSchemaT")


class Cache(Generic[BucketSchemaT]):
    """Type-safe, dynamic, two-layers caching system.
    Supports various eviction strategies: (Read Quota, LRU, LFU, FIFO).
    """

    def __init__(
        self,
        schema_type: type[BucketSchemaT],
        policy: CachePolicy | None = None,
    ) -> None:
        """Initialize the cache with a schema type and policy.

        Args:
            schema_type (type[BucketSchemaT]): schema type.
            policy (CachePolicy | None, optional): Set evection policy, and size limits. Defaults to None.
        """
        self._adapter = create_schema_adapter(schema_type)
        self._schema_type: type[BucketSchemaT] = schema_type
        self._store: dict[str, dict[str, CacheEntry[Any]]] = {}
        self._policy = policy or CachePolicy()
        self._bucket_sizes: dict[str, int] = {}
        self._bucket_last_accessed: dict[str, float] = {}

    def get_bucket(self, key: str) -> (
        BucketProxy[BucketSchemaT] | BucketSchemaT
    ) | None:
        """
        Retrieve a bucket from the cache.

        Returns a proxy that behaves exactly like the schema type with full
        type hints. All attribute access is tracked via CacheEntry metadata.

        Args:
            key: Cache key

        Returns:
            Proxy to the cached bucket, or None if not found
        """
        internal = self._store.get(key)
        if internal is None:
            return None

        self._bucket_last_accessed[key] = time.perf_counter()
        return self._create_proxy(key, internal)

    def set_bucket(
        self,
        key: str,
        bucket: BucketSchemaT,
        quota: int | None = None,
        trim: bool = True,
    ) -> bool:
        """
        Store a schema instance in the cache with an optional per-bucket read quota.
        Args:
            key: Cache key
            bucket: Instance of the schema type to cache
            quota: Optional read quota for all entries (each) in this bucket.
                Overrides policy default.
            trim: evict bucket entries if bucket size exceeds per-bucket limit (cache policy)
        """
        # Ensure we have space for a new bucket if it's new
        if key not in self._store:
            self._ensure_cache_capacity()

        # If quota is not provided, use policy default
        effective_quota = (
            quota if quota is not None else self._policy.delete_after_reads
        )

        internal = self._create_internal_map(bucket, quota=effective_quota)
        bucket_size = sum(entry.size for entry in internal.values())

        if self._policy.max_bucket_size and bucket_size > self._policy.max_bucket_size:
            if not trim:
                return False
            needs_trimming = True
        else:
            needs_trimming = False

        self._store[key] = internal
        self._bucket_sizes[key] = bucket_size
        self._bucket_last_accessed[key] = time.perf_counter()

        if needs_trimming or not self._policy.max_bucket_size:
            self._ensure_bucket_capacity(key, 0)

        return True

    def get_or_create(self, key: str) -> BucketProxy[BucketSchemaT] | BucketSchemaT:
        """
        Get an existing bucket or create a new empty one if it doesn't exist.

        Args:
            key: Cache key

        Returns:
            Existing or newly created bucket proxy
        """
        bucket = self.get_bucket(key)
        if bucket is not None:
            return bucket

        # Create and store new bucket
        self._ensure_cache_capacity()
        field_names = self._adapter.get_field_names()
        internal = {
            name: CacheEntry.create(
                None, remaining_reads=self._policy.delete_after_reads
            )
            for name in field_names
        }
        self._store[key] = internal
        self._bucket_sizes[key] = sum(entry.size for entry in internal.values())
        self._bucket_last_accessed[key] = time.perf_counter()
        return self._create_proxy(key, internal)

    def delete_bucket(self, key: str) -> bool:
        """
        Remove a bucket from the cache.
        Args:
            key: Cache key
        Returns:
            True if the bucket existed and was deleted, False otherwise
        """
        if key in self._store:
            del self._store[key]
            self._bucket_sizes.pop(key, None)
            self._bucket_last_accessed.pop(key, None)
            return True
        return False

    def keys(self) -> list[str]:
        """Return all cache keys."""
        return list(self._store.keys())

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        return key in self._store

    def __len__(self) -> int:
        """Return the number of cached buckets."""
        return len(self._store)

    def __getitem__(self, key: str) -> (
        BucketProxy[BucketSchemaT] | BucketSchemaT
    ) | None:
        return self.get_bucket(key)

    def __setitem__(self, key: str, bucket: BucketSchemaT) -> None:
        self.set_bucket(key, bucket)

    def __delitem__(self, key: str) -> None:
        if not self.delete_bucket(key):
            raise KeyError(f"Cache key '{key}' not found")

    def get_raw_internal(self, key: str) -> dict[str, CacheEntry[Any]] | None:
        """
        Access the raw internal CacheEntry map for advanced use cases.
        Args:
            key: Cache key
        Returns:
            Internal map of field names to CacheEntry objects, or None if not found
        """
        return self._store.get(key)

    def get_field_entry(self, key: str, field_name: str) -> CacheEntry[Any]:
        """
        Get the CacheEntry for a specific field.
        Useful for inspecting metadata like read_count, size, and created_at.

            key: Cache key
            field_name: Name of the field
        Returns:
            CacheEntry for the field
        Raises:
            KeyError: If bucket doesn't exist
            AttributeError: If field doesn't exist in schema
        """
        internal = self._store.get(key)
        if internal is None:
            raise KeyError(f"Bucket with key '{key}' not found")
        if field_name not in internal:
            raise AttributeError(
                f"'{self._schema_type.__name__}' has no field '{field_name}'"
            )
        return internal[field_name]

    def set_entry(
        self, key: str, entry_name: str, value: Any, quota: int | None = None
    ) -> None:
        """
        Update a single field on a cached bucket with an optional per-entry quota.
        Args:
            key: Cache key
            entry_name: Name of field to update
            value: New value for the field
            quota: Optional read quota for this specific entry.
                Overrides bucket/policy defaults.
        Raises:
            KeyError: If bucket doesn't exist
            AttributeError: If field doesn't exist in schema
            ValueError: If Pydantic validation fails or size limit exceeded
        """
        internal = self._store.get(key)
        if internal is None:
            raise KeyError(f"Bucket with key '{key}' not found")
        if entry_name not in internal:
            raise AttributeError(
                f"'{self._schema_type.__name__}' has no field '{entry_name}'"
            )

        # Validate value
        validated_value = self._adapter.validate_field(entry_name, value)

        # Determine quota (passed quota > policy default)
        effective_quota = (
            quota if quota is not None else self._policy.delete_after_reads
        )

        self._process_entry_write(key, entry_name, validated_value, effective_quota)

    def get_field(self, key: str, field_name: str) -> Any:
        """
        Retrieve a single field value from a cached bucket.
        Handles TTL and Quota eviction.
        """
        internal = self._store.get(key)
        if internal is None:
            return None
        if field_name not in internal:
            raise AttributeError(
                f"'{self._schema_type.__name__}' has no field '{field_name}'"
            )

        entry = internal[field_name]
        entry.touch()
        return self._process_entry_read(key, field_name, entry)

    def _process_entry_read(
        self, key: str, field_name: str, entry: CacheEntry[Any]
    ) -> Any:  # noqa: ARG002
        """Centralized read logic for both Proxy and direct Cache access."""
        self._bucket_last_accessed[key] = time.perf_counter()

        # 1. Check TTL expiration - Strict
        if (
            self._policy.entry_ttl
            and time.perf_counter() - entry.created_at > self._policy.entry_ttl
        ):
            old_size = entry.size
            entry.update(None)
            self._bucket_sizes[key] -= old_size - entry.size
            return None

        val = entry.value

        # 2. Check remaining reads - Lazy
        if entry.remaining_reads is not None:
            if entry.remaining_reads < 0:
                if val is not None:
                    old_size = entry.size
                    entry.update(None, remaining_reads=entry.remaining_reads)
                    self._bucket_sizes[key] -= old_size - entry.size
                return None

        return val

    def _process_entry_write(
        self,
        key: str,
        field_name: str,
        value: Any,
        quota: int | None,
    ) -> None:
        """Centralized write logic for both Proxy and direct Cache access."""
        # 1. Pre-check size
        val_size = sys.getsizeof(value)
        if self._policy.max_entry_size and val_size > self._policy.max_entry_size:
            raise ValueError(f"Value for field '{field_name}' exceeds max_entry_size")

        bucket = self._store.get(key)
        if bucket is None:
            raise KeyError(f"Cache store doesn't have the bucket for this key: [{key}]")

        entry = bucket[field_name]
        size_diff = val_size - entry.size

        # 2. Ensure capacity
        self._ensure_bucket_capacity(key, size_diff)
        self._bucket_sizes[key] += size_diff
        self._bucket_last_accessed[key] = time.perf_counter()

        # 3. Update entry
        quota = quota if quota is not None else self._policy.delete_after_reads
        entry.update(value, remaining_reads=quota)

    def _create_proxy(
        self, key: str, internal: dict[str, CacheEntry[Any]]
    ) -> BucketProxy[BucketSchemaT]:
        """Create a BucketProxy with appropriate callbacks for this cache instance."""

        def on_read(field_name: str, entry: CacheEntry[Any]) -> Any:
            return self._process_entry_read(key, field_name, entry)

        def on_write(
            field_name: str,
            value: Any,
            quota: int | None = None,
        ) -> None:
            self._process_entry_write(
                key=key,
                field_name=field_name,
                value=value,
                quota=quota,
            )

        return BucketProxy(internal, self._adapter, on_read=on_read, on_write=on_write)

    def clear(self) -> None:
        """Remove all buckets from the cache."""
        self._store.clear()
        self._bucket_sizes.clear()
        self._bucket_last_accessed.clear()

    def _ensure_cache_capacity(self) -> None:
        """Ensure cache doesn't exceed max_buckets limit."""
        if self._policy.max_buckets is None:
            return

        while len(self._store) >= self._policy.max_buckets:
            evict_key = self._select_bucket_for_eviction()
            if evict_key:
                self.delete_bucket(evict_key)
            else:
                break

    def _select_bucket_for_eviction(self) -> str | None:
        """Select a bucket to evict (LRU based on bucket access)."""
        if not self._store:
            return None
        return min(
            self._store.keys(), key=lambda k: self._bucket_last_accessed.get(k, 0)
        )

    def _ensure_bucket_capacity(self, key: str, required_space: int) -> None:
        """Evict fields within a bucket to make room for new data."""
        if self._policy.max_bucket_size is None:
            return

        internal = self._store.get(key)
        if not internal:
            return

        while (
            self._bucket_sizes.get(key, 0) + required_space
            > self._policy.max_bucket_size
        ):
            field_to_evict = self._select_entry_for_eviction(key)
            if field_to_evict:
                entry = internal[field_to_evict]
                old_size = entry.size
                entry.update(None)  # "Evict" by setting value to None
                self._bucket_sizes[key] -= old_size - entry.size
            else:
                # If we can't evict anything else and still no space
                if self._policy.eviction_strategy == "none":
                    raise RuntimeError(
                        f"Bucket '{key}' exceeded max_bucket_size and no eviction strategy set"
                    )
                break

    def _select_entry_for_eviction(self, key: str) -> str | None:
        """Select an entry within a bucket to evict based on strategy."""
        internal = self._store.get(key)
        if not internal:
            return None

        # Only evict fields that have a value
        entries = [f for f, e in internal.items() if e.value is not None]
        if not entries:
            return None

        strategy = self._policy.eviction_strategy
        if strategy == "lru":
            return min(entries, key=lambda entry: internal[entry].last_accessed_at)
        elif strategy == "lfu":
            return min(entries, key=lambda entry: internal[entry].read_count)
        elif strategy == "fifo":
            return min(entries, key=lambda entry: internal[entry].created_at)
        elif strategy == "read_quota":
            return min(
                entries,
                key=lambda entry: (
                    internal[entry].remaining_reads
                    if internal[entry].remaining_reads is not None
                    else float("inf")
                ),
            )
        else:
            raise ValueError(f"eviction strategy: [{strategy}] isn't supported")

    def _create_internal_map(
        self, instance: BucketSchemaT, quota: int | None = None
    ) -> dict[str, CacheEntry[Any]]:
        """Convert a schema instance into an internal CacheEntry map."""
        values = self._adapter.extract_values(instance)
        effective_quota = (
            quota if quota is not None else self._policy.delete_after_reads
        )
        return {
            name: CacheEntry.create(value, remaining_reads=effective_quota)
            for name, value in values.items()
        }
