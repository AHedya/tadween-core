from dataclasses import dataclass
from typing import Literal


@dataclass
class CachePolicy:
    """
    Configuration for cache behavior and limits. All sizes are in bytes

    Size Limits:
        max_entry_size: Maximum size for a single entry value in bytes
        max_bucket_size: Maximum total size for all entries in a bucket in bytes
        max_buckets: Maximum number of buckets in the cache

    Eviction:
        eviction_strategy: How to evict entries/buckets when limits are reached
            - "lru": Least Recently Used (based on last_accessed_at)
            - "lfu": Least Frequently Used (based on read_count)
            - "fifo": First In First Out (based on created_at)
            - "read_quota": Entry with least remaining reads (based on remaining_reads)
            - "none": No automatic eviction (raises error on overflow)

    Behavior:
        delete_on_read: If True, entries are deleted after being read once
        delete_after_reads: Number of reads after which entry is deleted (None = infinite)
        entry_ttl: Time to live for entries in seconds (None = no expiration)
    """

    # Size limits
    max_entry_size: int | None = None
    max_bucket_size: int | None = None
    max_buckets: int | None = None

    # Eviction strategy
    eviction_strategy: Literal["lru", "lfu", "fifo", "read_quota", "none"] = "none"

    # Behavioral options
    delete_on_read: bool = False
    delete_after_reads: int | None = None
    entry_ttl: float | None = None

    def __post_init__(self) -> None:
        """Validate policy configuration."""
        if self.max_entry_size is not None and self.max_entry_size <= 0:
            raise ValueError("max_entry_size must be positive")
        if self.max_bucket_size is not None and self.max_bucket_size <= 0:
            raise ValueError("max_bucket_size must be positive")
        if self.max_buckets is not None and self.max_buckets <= 0:
            raise ValueError("max_buckets must be positive")
        if self.entry_ttl is not None and self.entry_ttl <= 0:
            raise ValueError("entry_ttl must be positive")
        if self.delete_after_reads is not None and self.delete_after_reads <= 0:
            raise ValueError("delete_after_reads must be positive")

        if self.delete_on_read and self.delete_after_reads is None:
            self.delete_after_reads = 1
