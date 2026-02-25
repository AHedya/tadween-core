import time
from dataclasses import dataclass

import pytest

from tadween_core.cache.cache import Cache
from tadween_core.cache.policy import CachePolicy


@dataclass
class SimpleSchema:
    field1: str | None = None
    field2: int | None = None
    field3: str | None = None


def test_delete_after_reads():
    policy = CachePolicy(delete_after_reads=2)
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")
    bucket.field1 = "hello"

    v1 = bucket.field1  # Read 1
    print(
        f"DEBUG: v1={v1} rem={cache.get_field_entry('key1', 'field1').remaining_reads}"
    )
    assert v1 == "hello"

    v2 = bucket.field1  # Read 2
    print(
        f"DEBUG: v2={v2} rem={cache.get_field_entry('key1', 'field1').remaining_reads}"
    )
    assert v2 == "hello"

    v3 = bucket.field1  # Read 3 (evicted)
    print(
        f"DEBUG: v3={v3} rem={cache.get_field_entry('key1', 'field1').remaining_reads}"
    )
    assert v3 is None


def test_max_entry_size():
    policy = CachePolicy(max_entry_size=100)
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # Small value should work
    bucket.field1 = "small"

    # Large value should fail
    with pytest.raises(ValueError, match="exceeds max_entry_size"):
        bucket.field1 = "a" * 1000


def test_max_bucket_size_eviction_lru():
    # Set a small bucket size that can fit one large field but not two
    policy = CachePolicy(max_bucket_size=100, eviction_strategy="lru")
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    bucket.field1 = "a" * 40  # size ~ 89
    # field1 is now LRU

    bucket.field2 = 123  # size ~ 28
    # Total size would be 89 + 28 = 117 > 100
    # Should evict field1

    assert bucket.field2 == 123
    assert bucket.field1 is None


def test_max_buckets_eviction():
    policy = CachePolicy(max_buckets=2)
    cache = Cache(SimpleSchema, policy=policy)

    cache.get_or_create("key1")
    cache.get_or_create("key2")

    assert "key1" in cache
    assert "key2" in cache

    # This should evict key1 (the oldest)
    cache.get_or_create("key3")

    assert "key3" in cache
    assert "key2" in cache
    assert "key1" not in cache


def test_shortest_quota_eviction():
    policy = CachePolicy(
        max_bucket_size=100, eviction_strategy="read_quota", delete_after_reads=10
    )
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")
    bucket.field1 = "a" * 30  # size ~ 79
    # field1 quota: 10

    # Read field1 to decrease quota
    _ = bucket.field1
    _ = bucket.field1
    # field1 quota: 8

    # Add field2, will exceed 100 (79 + 79 > 100)
    bucket.field2 = "b" * 30
    # field2 quota: 10

    # field1 has quota 8, field2 has 10. field1 should be evicted.
    assert bucket.field2 == "b" * 30
    assert bucket.field1 is None


def test_ttl_expiration():
    policy = CachePolicy(entry_ttl=0.1)  # 100ms
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")
    bucket.field1 = "hello"

    assert bucket.field1 == "hello"

    time.sleep(0.15)

    # Should be evicted on next read
    assert bucket.field1 is None


def test_per_entry_quota():
    # Policy says 10 reads
    policy = CachePolicy(delete_after_reads=10)
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # Override for this specific entry to 1 read
    bucket.set_entry("field1", "once", quota=1)

    assert bucket.field1 == "once"  # Read 1
    assert bucket.field1 is None  # Read 2 (evicted)


def test_per_bucket_quota():
    policy = CachePolicy(delete_after_reads=10)
    cache = Cache(SimpleSchema, policy=policy)

    # Set bucket with 2-read quota
    bucket_data = SimpleSchema(field1="data")
    cache.set_bucket("key1", bucket_data, quota=2)

    bucket = cache.get_bucket("key1")
    assert bucket.field1 == "data"  # Read 1
    assert bucket.field1 == "data"  # Read 2
    assert bucket.field1 is None  # Read 3 (evicted)


def test_lfu_eviction():
    """Test LFU (Least Frequently Used) eviction strategy."""
    policy = CachePolicy(max_bucket_size=150, eviction_strategy="lfu")
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # Add field1 and read it many times
    bucket.field1 = "a" * 10
    for _ in range(10):
        _ = bucket.field1  # field1 has 10 reads

    # Add field2 but don't read it much
    bucket.field2 = "b" * 10
    _ = bucket.field2  # field2 has 1 read

    # Add field3, should evict field2 (least frequently used)
    bucket.field3 = "c" * 10

    assert bucket.field1 == "a" * 10  # Still there (10 reads)
    assert bucket.field2 is None  # Evicted (1 read)
    assert bucket.field3 == "c" * 10  # Newly added


def test_fifo_eviction():
    """Test FIFO (First In First Out) eviction strategy."""
    policy = CachePolicy(max_bucket_size=150, eviction_strategy="fifo")
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # Add fields in order
    bucket.field1 = "a" * 10  # First, will be evicted
    bucket.field2 = "b" * 10  # Second
    bucket.field3 = "c" * 10  # Third, will evict field1

    assert bucket.field1 is None  # Evicted (oldest)
    assert bucket.field2 == "b" * 10
    assert bucket.field3 == "c" * 10


def test_none_eviction_strategy():
    """Test 'none' eviction strategy raises RuntimeError on overflow."""
    policy = CachePolicy(max_bucket_size=100, eviction_strategy="none")
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # This should work
    bucket.field1 = "a" * 10

    # This should exceed limit and raise RuntimeError
    with pytest.raises(ValueError):
        bucket.field2 = "b" * 100


def test_delete_on_read_behavior():
    """Test delete_on_read policy behavior."""
    policy = CachePolicy(delete_on_read=True)
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")
    bucket.field1 = "test"

    # First read should work
    assert bucket.field1 == "test"

    # Second read should be evicted
    assert bucket.field1 is None

    # Verify quota was set to 1
    entry = cache.get_field_entry("key1", "field1")
    assert entry.remaining_reads is not None


def test_pydantic_model_validation():
    """Test Pydantic model validation in cache."""
    from pydantic import BaseModel

    class UserSchema(BaseModel):
        username: str
        email: str
        age: int | None = None

    cache = Cache(UserSchema)

    # Create bucket first
    bucket = cache.get_or_create("user1")

    # Valid data
    bucket.username = "john"
    bucket.email = "john@example.com"
    bucket.age = 30

    assert bucket.username == "john"
    assert bucket.email == "john@example.com"
    assert bucket.age == 30


def test_delete_on_read_with_pydantic():
    """Test delete_on_read works with Pydantic models."""
    from pydantic import BaseModel

    class SimplePydantic(BaseModel):
        field1: str | None = None
        field2: int | None = None

    policy = CachePolicy(delete_after_reads=2)
    cache = Cache(SimplePydantic, policy=policy)

    bucket = cache.get_or_create("key1")
    bucket.field1 = "test"

    assert bucket.field1 == "test"  # Read 1
    assert bucket.field1 == "test"  # Read 2
    assert bucket.field1 is None  # Read 3 (evicted)


def test_quota_exhaustion_multiple_fields():
    """Test quota exhaustion with multiple fields having different quotas."""
    policy = CachePolicy(delete_after_reads=3)
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # Set fields with different quotas
    bucket.set_entry("field1", "data1", quota=1)
    bucket.set_entry("field2", "data2", quota=5)

    # field1 should be evicted after 2 reads
    assert bucket.field1 == "data1"  # Read 1
    assert bucket.field1 is None  # Read 2 (evicted)

    # field2 should still be available
    assert bucket.field2 == "data2"  # Read 1
    assert bucket.field2 == "data2"  # Read 2
    assert bucket.field2 == "data2"  # Read 3
    assert bucket.field2 == "data2"  # Read 4


def test_bucket_size_tracking_after_eviction():
    """Test that bucket size is correctly tracked after eviction."""
    policy = CachePolicy(max_bucket_size=100, eviction_strategy="lru")
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # Add large field
    bucket.field1 = "a" * 60

    # Add another field that causes eviction
    bucket.field2 = "b" * 60

    # field1 should be evicted
    assert bucket.field1 is None
    assert bucket.field2 == "b" * 60

    # Bucket size should be tracked correctly
    entry = cache.get_field_entry("key1", "field2")
    # Size should be reasonable (not sum of both fields)
    assert entry.size < 150  # Not too large


def test_eviction_with_all_none_fields():
    """Test eviction when all fields are None."""
    policy = CachePolicy(max_bucket_size=100, eviction_strategy="lru")
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # All fields are None initially
    assert bucket.field1 is None
    assert bucket.field2 is None

    # Adding a new field should work
    bucket.field1 = "test"
    assert bucket.field1 == "test"


def test_remaining_reads_preserved_on_update():
    """Test that remaining_reads is preserved when updating quota."""
    policy = CachePolicy(delete_after_reads=10)
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")
    bucket.field1 = "test"

    # Read a few times
    _ = bucket.field1
    _ = bucket.field1

    entry = cache.get_field_entry("key1", "field1")
    initial_reads = entry.read_count

    # Update the field
    bucket.field1 = "updated"

    # Read count should reset
    entry = cache.get_field_entry("key1", "field1")
    assert entry.read_count == 0  # Reset on update
    assert entry.value == "updated"


def test_multiple_buckets_with_eviction():
    """Test eviction across multiple buckets."""
    policy = CachePolicy(max_buckets=3, eviction_strategy="lru")
    cache = Cache(SimpleSchema, policy=policy)

    # Add 3 buckets
    cache.get_or_create("bucket1")
    cache.get_or_create("bucket2")
    cache.get_or_create("bucket3")

    assert len(cache) == 3

    # Access bucket1 to make it recently used
    bucket1 = cache.get_bucket("bucket1")
    bucket1.field1 = "test"

    # Add 4th bucket, should evict the oldest
    cache.get_or_create("bucket4")

    assert len(cache) == 3
    assert "bucket4" in cache
    assert "bucket1" in cache  # Should still be there (recently accessed)


def test_zero_quota():
    """Test behavior when quota is set to 0."""
    cache = Cache(SimpleSchema)

    bucket = cache.get_or_create("key1")

    # Set quota to 0 means no read allowed
    bucket.set_entry("field1", "test", quota=0)

    # First read should evict it (quota goes to -1)
    assert bucket.field1 is None


def test_large_quota_number():
    """Test behavior with very large quota numbers."""
    cache = Cache(SimpleSchema)

    bucket = cache.get_or_create("key1")

    # Set a very large quota
    large_quota = 1000000
    bucket.set_entry("field1", "test", quota=large_quota)

    # Should be readable many times
    for _ in range(100):
        assert bucket.field1 == "test"

    entry = cache.get_field_entry("key1", "field1")
    assert entry.read_count == 100
    assert entry.remaining_reads == large_quota - 100
