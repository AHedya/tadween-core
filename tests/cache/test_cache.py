import pytest

from tadween_core.cache.cache import Cache
from tadween_core.cache.policy import CachePolicy

from .shared import SimpleSchema


def test_cache_basic_crud(cache):
    """Test basic create, read, update, delete operations."""
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


def test_get_or_create(cache):
    """Test get_or_create returns existing bucket or creates new one."""
    # Create new bucket
    bucket1 = cache.get_or_create("key1")
    assert bucket1 is not None
    assert bucket1.field1 is None

    # Get existing bucket
    bucket1.field1 = "test"
    bucket2 = cache.get_or_create("key1")
    assert bucket2.field1 == "test"
    # Note: Proxies are fresh objects but point to same internal state


def test_dictionary_interface_contains(cache):
    """Test __contains__ method."""
    assert "key1" not in cache

    bucket_data = SimpleSchema(field1="test")
    cache.set_bucket("key1", bucket_data)

    assert "key1" in cache
    assert "key2" not in cache


def test_dictionary_interface_len(cache):
    """Test __len__ method."""
    assert len(cache) == 0

    cache.get_or_create("key1")
    assert len(cache) == 1

    cache.get_or_create("key2")
    assert len(cache) == 2

    cache.delete_bucket("key1")
    assert len(cache) == 1

    cache.clear()
    assert len(cache) == 0


def test_dictionary_interface_keys(cache):
    """Test keys() method."""
    assert cache.keys() == []

    cache.get_or_create("key1")
    cache.get_or_create("key2")
    cache.get_or_create("key3")

    keys = cache.keys()
    assert len(keys) == 3
    assert "key1" in keys
    assert "key2" in keys
    assert "key3" in keys


def test_dictionary_interface_getitem(cache):
    """Test __getitem__ method."""
    bucket_data = SimpleSchema(field1="test")
    cache.set_bucket("key1", bucket_data)

    bucket = cache["key1"]
    assert bucket is not None
    assert bucket.field1 == "test"

    assert cache["nonexistent"] is None


def test_dictionary_interface_setitem(cache):
    """Test __setitem__ method."""
    bucket_data = SimpleSchema(field1="test")
    cache["key1"] = bucket_data

    bucket = cache.get_bucket("key1")
    assert bucket is not None
    assert bucket.field1 == "test"


def test_dictionary_interface_delitem(cache):
    """Test __delitem__ method."""
    bucket_data = SimpleSchema(field1="test")
    cache.set_bucket("key1", bucket_data)

    del cache["key1"]
    assert cache.get_bucket("key1") is None

    with pytest.raises(KeyError, match="Cache key 'key1' not found"):
        del cache["key1"]


def test_clear(cache):
    """Test clear() method removes all buckets."""
    cache.get_or_create("key1")
    cache.get_or_create("key2")
    cache.get_or_create("key3")

    assert len(cache) == 3

    cache.clear()

    assert len(cache) == 0
    assert cache.get_bucket("key1") is None
    assert cache.get_bucket("key2") is None
    assert cache.get_bucket("key3") is None


def test_get_field(cache):
    """Test get_field() method for direct field access."""
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


def test_get_field_entry(cache):
    """Test get_field_entry() method for metadata inspection."""
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


def test_get_raw_internal(cache):
    """Test get_raw_internal() for advanced access."""
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


def test_set_entry(cache):
    """Test set_entry() method for direct field updates."""
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


def test_proxy_set_entry(cache):
    """Test proxy set_entry() method."""
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


def test_proxy_to_instance(cache):
    """Test proxy to_instance() method."""
    bucket_data = SimpleSchema(field1="hello", field2=42)
    cache.set_bucket("key1", bucket_data)

    bucket = cache.get_bucket("key1")

    # Convert to actual instance
    instance = bucket.to_instance()

    assert isinstance(instance, SimpleSchema)
    assert instance.field1 == "hello"
    assert instance.field2 == 42
    assert instance is not bucket  # Should be different object


def test_proxy_get_entry(cache):
    """Test proxy get_entry() method."""
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


def test_proxy_repr(cache):
    """Test proxy __repr__ method."""
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
    policy = CachePolicy(max_bucket_size=400, eviction_strategy="lru")
    cache = Cache(SimpleSchema, policy=policy)

    try:
        bucket = cache.get_or_create("key1")

        # Add fields in order. ASCII char * 10 ~= 149
        bucket.field1 = "a" * 100
        bucket.field2 = "b" * 100
        # All fields should be present with larger limit
        assert bucket.field1 is not None
        assert bucket.field2 is not None
        assert bucket.field3 is None

        bucket.field3 = "c" * 100

        assert bucket.field1 is None
        assert bucket.field2 is not None
        assert bucket.field3 is not None
    finally:
        cache.clear()


def test_empty_bucket_fields(cache):
    """Test that empty bucket has all fields with None values."""
    bucket = cache.get_or_create("key1")

    assert bucket.field1 is None
    assert bucket.field2 is None
    assert bucket.field3 is None


def test_update_preserves_metadata(cache):
    """Test that updating a field resets its metadata."""
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


def test_manual_evict_entry_cache(cache):
    """Test manual eviction of an entry via Cache object."""
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


def test_manual_evict_entry_proxy(cache):
    """Test manual eviction of an entry via BucketProxy."""
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


def test_evict_bucket_alias(cache):
    """Test evict_bucket alias."""
    cache.set_bucket("key1", SimpleSchema(field1="hello"))

    assert "key1" in cache
    assert cache.evict_bucket("key1") is True
    assert "key1" not in cache


def test_evict_non_existent(cache):
    """Test eviction of non-existent keys/fields."""
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
