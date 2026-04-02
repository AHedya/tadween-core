from __future__ import annotations

import contextlib
import sys
import time
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

V = TypeVar("V")
# split factory generic from class generic
T = TypeVar("T")
BucketSchemaT = TypeVar("BucketSchemaT")


class BaseCache(Protocol[BucketSchemaT]):
    @property
    def lock(self) -> contextlib.AbstractContextManager[Any]: ...

    @property
    def schema_type(self) -> type[BucketSchemaT]: ...

    def get_bucket(self, key: str) -> BucketSchemaT | None: ...
    def set_bucket(self, key: str, bucket: BucketSchemaT, **kwargs: Any) -> bool: ...
    def delete_bucket(self, key: str) -> None: ...
    def clear(self) -> None: ...
    def keys(self) -> list[str]: ...
    def __contains__(self, key: str) -> bool: ...
    def __getitem__(self, key: str) -> BucketSchemaT | None: ...
    def __setitem__(self, key: str, bucket: BucketSchemaT) -> None: ...
    def __delitem__(self, key: str) -> None: ...
    def __len__(self) -> int: ...


def get_rounded_time(digits: int):
    return round(time.perf_counter(), digits)


@dataclass
class CacheEntry(Generic[V]):
    value: V
    size: int
    created_at: float
    last_accessed_at: float
    read_count: int = 0
    remaining_reads: int | None = None

    @classmethod
    def create(cls, value: T, remaining_reads: int | None = None) -> CacheEntry[T]:
        now = get_rounded_time(5)
        return cls(
            value=value,
            size=sys.getsizeof(value),
            created_at=now,
            last_accessed_at=now,
            remaining_reads=remaining_reads,
        )

    def touch(self) -> None:
        """Updates read count and timestamp on access."""
        self.read_count += 1
        self.last_accessed_at = get_rounded_time(5)
        if self.remaining_reads is not None:
            self.remaining_reads -= 1

    def update(self, value: V, remaining_reads: int | None = None) -> None:
        """Update the cached value and reset metadata."""
        self.value = value
        self.size = sys.getsizeof(value)
        self.created_at = get_rounded_time(5)
        self.last_accessed_at = self.created_at
        self.read_count = 0
        self.remaining_reads = remaining_reads

    def is_expired(self, ttl: float | None) -> bool:
        if ttl is None:
            return False
        age = time.perf_counter() - self.created_at
        return age > ttl
