from dataclasses import dataclass

import pytest

from tadween_core.cache.cache import Cache
from tadween_core.cache.policy import CachePolicy


@dataclass
class SimpleSchema:
    field1: str | None = None
    field2: int | None = None
    field3: list | None = None


def test_cache_basic_crud():
    """Test basic create, read, update, delete operations."""
    cache = Cache(SimpleSchema)

    # Create
    bucket_data = SimpleSchema(field1="hello", field2=42)
    assert cache.set_bucket("key1", bucket_data) is True

    # Read
    bucket = cache.get_bucket("key1")

    assert bucket is not None
    assert bucket.field1 == "hello"
    assert bucket.field2 == 42

    # Update via proxy
    bucket.field1 = "world"
    bucket.field2 = 100
    assert bucket.field1 == "world"
    assert bucket.field2 == 100

    # Delete
    assert cache.delete_bucket("key1") is True
    assert cache.get_bucket("key1") is None
    assert cache.delete_bucket("key1") is False


def test_get_or_create():
    """Test get_or_create returns existing bucket or creates new one."""
    cache = Cache(SimpleSchema)

    # Create new bucket
    bucket1 = cache.get_or_create("key1")
    assert bucket1 is not None
    assert bucket1.field1 is None

    # Get existing bucket
    bucket1.field1 = "test"
    bucket2 = cache.get_or_create("key1")
    assert bucket2.field1 == "test"
    # Note: Proxies are fresh objects but point to same internal state


def test_dictionary_interface_contains():
    """Test __contains__ method."""
    cache = Cache(SimpleSchema)

    assert "key1" not in cache

    bucket_data = SimpleSchema(field1="test")
    cache.set_bucket("key1", bucket_data)

    assert "key1" in cache
    assert "key2" not in cache


def test_dictionary_interface_len():
    """Test __len__ method."""
    cache = Cache(SimpleSchema)

    assert len(cache) == 0

    cache.get_or_create("key1")
    assert len(cache) == 1

    cache.get_or_create("key2")
    assert len(cache) == 2

    cache.delete_bucket("key1")
    assert len(cache) == 1

    cache.clear()
    assert len(cache) == 0


def test_dictionary_interface_keys():
    """Test keys() method."""
    cache = Cache(SimpleSchema)

    assert cache.keys() == []

    cache.get_or_create("key1")
    cache.get_or_create("key2")
    cache.get_or_create("key3")

    keys = cache.keys()
    assert len(keys) == 3
    assert "key1" in keys
    assert "key2" in keys
    assert "key3" in keys


def test_dictionary_interface_getitem():
    """Test __getitem__ method."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="test")
    cache.set_bucket("key1", bucket_data)

    bucket = cache["key1"]
    assert bucket is not None
    assert bucket.field1 == "test"

    assert cache["nonexistent"] is None


def test_dictionary_interface_setitem():
    """Test __setitem__ method."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="test")
    cache["key1"] = bucket_data

    bucket = cache.get_bucket("key1")
    assert bucket is not None
    assert bucket.field1 == "test"


def test_dictionary_interface_delitem():
    """Test __delitem__ method."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="test")
    cache.set_bucket("key1", bucket_data)

    del cache["key1"]
    assert cache.get_bucket("key1") is None

    with pytest.raises(KeyError, match="Cache key 'key1' not found"):
        del cache["key1"]


def test_clear():
    """Test clear() method removes all buckets."""
    cache = Cache(SimpleSchema)

    cache.get_or_create("key1")
    cache.get_or_create("key2")
    cache.get_or_create("key3")

    assert len(cache) == 3

    cache.clear()

    assert len(cache) == 0
    assert cache.get_bucket("key1") is None
    assert cache.get_bucket("key2") is None
    assert cache.get_bucket("key3") is None


def test_get_field():
    """Test get_field() method for direct field access."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="hello", field2=42)
    cache.set_bucket("key1", bucket_data)

    # Get existing field
    assert cache.get_field("key1", "field1") == "hello"
    assert cache.get_field("key1", "field2") == 42

    # Get None field
    assert cache.get_field("key1", "field3") is None

    # Get from non-existent bucket
    assert cache.get_field("nonexistent", "field1") is None

    # Get non-existent field
    with pytest.raises(AttributeError, match="has no field 'invalid'"):
        cache.get_field("key1", "invalid")


def test_get_field_entry():
    """Test get_field_entry() method for metadata inspection."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="hello", field2=42)
    cache.set_bucket("key1", bucket_data)

    # Get entry metadata
    entry = cache.get_field_entry("key1", "field1")
    assert entry.value == "hello"
    assert entry.read_count == 0
    assert entry.size > 0
    assert entry.created_at > 0

    # After reading, read_count should increase
    _ = cache.get_field("key1", "field1")
    entry = cache.get_field_entry("key1", "field1")
    assert entry.read_count == 1

    # Non-existent bucket
    with pytest.raises(KeyError, match="Bucket with key 'nonexistent' not found"):
        cache.get_field_entry("nonexistent", "field1")

    # Non-existent field
    with pytest.raises(AttributeError, match="has no field 'invalid'"):
        cache.get_field_entry("key1", "invalid")


def test_get_raw_internal():
    """Test get_raw_internal() for advanced access."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="hello", field2=42)
    cache.set_bucket("key1", bucket_data)

    # Get internal map
    internal = cache.get_raw_internal("key1")
    assert internal is not None
    assert "field1" in internal
    assert "field2" in internal
    assert internal["field1"].value == "hello"
    assert internal["field2"].value == 42

    # Non-existent bucket
    assert cache.get_raw_internal("nonexistent") is None


def test_set_entry():
    """Test set_entry() method for direct field updates."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="original")
    cache.set_bucket("key1", bucket_data)

    # Update existing field
    cache.set_entry("key1", "field1", "updated")
    assert cache.get_field("key1", "field1") == "updated"

    # Update with quota (quota is decremented on read)
    cache.set_entry("key1", "field2", 123, quota=5)
    entry = cache.get_field_entry("key1", "field2")
    assert entry.remaining_reads == 5

    # Non-existent bucket
    with pytest.raises(KeyError, match="Bucket with key 'nonexistent' not found"):
        cache.set_entry("nonexistent", "field1", "value")

    # Non-existent field
    with pytest.raises(AttributeError, match="has no field 'invalid'"):
        cache.set_entry("key1", "invalid", "value")


def test_set_bucket_trim_false():
    """Test set_bucket with trim=False returns False when size exceeds limit."""
    policy = CachePolicy(max_bucket_size=50)
    cache = Cache(SimpleSchema, policy=policy)

    bucket_data = SimpleSchema(field1="a" * 100)
    result = cache.set_bucket("key1", bucket_data, trim=False)

    assert result is False
    assert cache.get_bucket("key1") is None


def test_proxy_set_entry():
    """Test proxy set_entry() method."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="original")
    cache.set_bucket("key1", bucket_data)

    bucket = cache.get_bucket("key1")

    # Set entry via proxy
    bucket.set_entry("field1", "updated")
    assert bucket.field1 == "updated"

    # Set entry with quota
    bucket.set_entry("field2", 42, quota=10)
    assert bucket.field2 == 42

    # Non-existent field
    with pytest.raises(AttributeError, match="has no attribute 'invalid'"):
        bucket.set_entry("invalid", "value")


def test_proxy_to_instance():
    """Test proxy to_instance() method."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="hello", field2=42)
    cache.set_bucket("key1", bucket_data)

    bucket = cache.get_bucket("key1")

    # Convert to actual instance
    instance = bucket.to_instance()

    assert isinstance(instance, SimpleSchema)
    assert instance.field1 == "hello"
    assert instance.field2 == 42
    assert instance is not bucket  # Should be different object


def test_proxy_get_entry():
    """Test proxy get_entry() method."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="hello")
    cache.set_bucket("key1", bucket_data)

    bucket = cache.get_bucket("key1")

    # Get entry metadata
    entry = bucket.get_entry("field1")
    assert entry.value == "hello"
    assert entry.read_count == 0

    # Read through proxy
    _ = bucket.field1
    entry = bucket.get_entry("field1")
    assert entry.read_count == 1

    # Non-existent field
    with pytest.raises(AttributeError, match="has no attribute 'invalid'"):
        bucket.get_entry("invalid")


def test_proxy_repr():
    """Test proxy __repr__ method."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="hello", field2=42)
    cache.set_bucket("key1", bucket_data)

    bucket = cache.get_bucket("key1")
    repr_str = repr(bucket)

    assert "BucketProxy" in repr_str
    assert "SimpleSchema" in repr_str
    assert "field1" in repr_str
    assert "field2" in repr_str


def test_multiple_fields_eviction_order():
    """Test that eviction happens when bucket size limit is exceeded."""
    policy = CachePolicy(max_bucket_size=140, eviction_strategy="lru")
    cache = Cache(SimpleSchema, policy=policy)

    bucket = cache.get_or_create("key1")

    # Add fields in order. ASCII char * 10 ~= 59
    bucket.field1 = "a" * 10
    bucket.field2 = "b" * 10

    # All fields should be present with larger limit
    assert bucket.field1 is not None
    assert bucket.field2 is not None
    assert bucket.field3 is None

    bucket.field3 = "c" * 10

    assert bucket.field1 is None
    assert bucket.field2 is not None
    assert bucket.field3 is not None


def test_empty_bucket_fields():
    """Test that empty bucket has all fields with None values."""
    cache = Cache(SimpleSchema)

    bucket = cache.get_or_create("key1")

    assert bucket.field1 is None
    assert bucket.field2 is None
    assert bucket.field3 is None


def test_update_preserves_metadata():
    """Test that updating a field resets its metadata."""
    cache = Cache(SimpleSchema)

    bucket_data = SimpleSchema(field1="original")
    cache.set_bucket("key1", bucket_data)

    # Read the field
    _ = cache.get_bucket("key1").field1

    entry = cache.get_field_entry("key1", "field1")
    assert entry.read_count == 1

    # Update the field
    cache.set_entry("key1", "field1", "updated")

    # Metadata should be reset
    entry = cache.get_field_entry("key1", "field1")
    assert entry.read_count == 0
    assert entry.value == "updated"
