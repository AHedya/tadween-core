from dataclasses import dataclass

import pytest

from tadween_core.cache.cache import Cache
from tadween_core.cache.policy import CachePolicy


@dataclass
class SimpleSchema:
    field1: str | None = None
    field2: int | None = None


def test_manual_evict_entry_cache():
    """Test manual eviction of an entry via Cache object."""
    cache = Cache(SimpleSchema)
    cache.set_bucket("key1", SimpleSchema(field1="hello", field2=42))

    initial_size = cache._bucket_sizes["key1"]

    # Evict field1
    assert cache.evict_entry("key1", "field1") is True
    assert cache.get_field("key1", "field1") is None
    assert cache.get_field("key1", "field2") == 42

    # Size should have decreased
    assert cache._bucket_sizes["key1"] < initial_size

    # Evict already empty field
    assert cache.evict_entry("key1", "field1") is False


def test_manual_evict_entry_proxy():
    """Test manual eviction of an entry via BucketProxy."""
    cache = Cache(SimpleSchema)
    bucket = cache.get_or_create("key1")
    bucket.field1 = "hello"
    bucket.field2 = 42

    initial_size = cache._bucket_sizes["key1"]

    # Evict field1 via proxy.evict()
    assert bucket.evict("field1") is True
    assert bucket.field1 is None
    assert bucket.field2 == 42
    assert cache._bucket_sizes["key1"] < initial_size

    # Evict field2 via del
    del bucket.field2
    assert bucket.field2 is None
    assert cache.get_field("key1", "field2") is None


def test_evict_bucket_alias():
    """Test evict_bucket alias."""
    cache = Cache(SimpleSchema)
    cache.set_bucket("key1", SimpleSchema(field1="hello"))

    assert "key1" in cache
    assert cache.evict_bucket("key1") is True
    assert "key1" not in cache


def test_evict_non_existent():
    """Test eviction of non-existent keys/fields."""
    cache = Cache(SimpleSchema)

    assert cache.evict_bucket("nonexistent") is False
    assert cache.evict_entry("nonexistent", "field1") is False

    bucket = cache.get_or_create("key1")
    with pytest.raises(AttributeError):
        bucket.evict("invalid_field")

    with pytest.raises(AttributeError):
        del bucket.invalid_field


def test_immediate_eviction_on_quota_reached():
    """Test that entry is evicted and size updated as soon as quota reaches 0."""
    policy = CachePolicy(delete_after_reads=1)
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")
    bucket.field1 = "hello"

    initial_bucket_size = cache._bucket_sizes["key1"]
    entry = cache.get_field_entry("key1", "field1")
    assert entry.value == "hello"
    assert entry.remaining_reads == 1

    # Read once - quota reaches 0
    val = bucket.field1
    assert val == "hello"

    # Entry should now be empty and size reduced IMMEDIATELY
    entry_after = cache.get_field_entry("key1", "field1")
    assert entry_after.value is None
    assert entry_after.remaining_reads == 0
    assert cache._bucket_sizes["key1"] < initial_bucket_size

    # Subsequent read returns None
    assert bucket.field1 is None
    assert entry_after.remaining_reads == -1
