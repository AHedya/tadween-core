import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from tadween_core.cache.simple_cache import SimpleCache

from .shared import AnotherSchema, SimpleSchema


class TestBasicCRUD:
    def test_set_and_get_bucket(self, simple_cache):
        bucket_data = SimpleSchema(field1="hello", field2=42)
        assert simple_cache.set_bucket("key1", bucket_data) is True

        result = simple_cache.get_bucket("key1")
        assert result is not None
        assert result.field1 == "hello"
        assert result.field2 == 42

    def test_get_nonexistent_bucket(self, simple_cache):
        result = simple_cache.get_bucket("nonexistent")
        assert result is None

    def test_update_bucket(self, simple_cache):
        bucket_data = SimpleSchema(field1="original")
        simple_cache.set_bucket("key1", bucket_data)

        updated_data = SimpleSchema(field1="updated", field2=100)
        simple_cache.set_bucket("key1", updated_data)

        result = simple_cache.get_bucket("key1")
        assert result is not None
        assert result.field1 == "updated"
        assert result.field2 == 100

    def test_delete_bucket(self, simple_cache):
        bucket_data = SimpleSchema(field1="test")
        simple_cache.set_bucket("key1", bucket_data)

        assert simple_cache.get_bucket("key1") is not None
        simple_cache.delete_bucket("key1")
        assert simple_cache.get_bucket("key1") is None

    def test_delete_nonexistent_bucket(self, simple_cache):
        assert simple_cache.delete_bucket("nonexistent") is None


class TestDictionaryInterface:
    def test_contains(self, simple_cache):
        assert "key1" not in simple_cache

        bucket_data = SimpleSchema(field1="test")
        simple_cache.set_bucket("key1", bucket_data)

        assert "key1" in simple_cache
        assert "key2" not in simple_cache

    def test_len(self, simple_cache):
        assert len(simple_cache) == 0

        simple_cache.set_bucket("key1", SimpleSchema())
        assert len(simple_cache) == 1

        simple_cache.set_bucket("key2", SimpleSchema())
        assert len(simple_cache) == 2

        simple_cache.delete_bucket("key1")
        assert len(simple_cache) == 1

        simple_cache.clear()
        assert len(simple_cache) == 0

    def test_getitem(self, simple_cache):
        bucket_data = SimpleSchema(field1="test")
        simple_cache.set_bucket("key1", bucket_data)

        bucket = simple_cache["key1"]
        assert bucket is not None
        assert bucket.field1 == "test"

        assert simple_cache["nonexistent"] is None

    def test_setitem(self, simple_cache):
        bucket_data = SimpleSchema(field1="test")
        simple_cache["key1"] = bucket_data

        result = simple_cache.get_bucket("key1")
        assert result is not None
        assert result.field1 == "test"

    def test_delitem(self, simple_cache):
        bucket_data = SimpleSchema(field1="test")
        simple_cache.set_bucket("key1", bucket_data)

        del simple_cache["key1"]
        assert simple_cache.get_bucket("key1") is None

        del simple_cache["key1"]  # no raise for not-existent


class TestClear:
    def test_clear_removes_all_buckets(self, simple_cache):
        simple_cache.set_bucket("key1", SimpleSchema())
        simple_cache.set_bucket("key2", SimpleSchema())
        simple_cache.set_bucket("key3", SimpleSchema())

        assert len(simple_cache) == 3

        simple_cache.clear()

        assert len(simple_cache) == 0
        assert simple_cache.get_bucket("key1") is None
        assert simple_cache.get_bucket("key2") is None
        assert simple_cache.get_bucket("key3") is None


class TestKeys:
    def test_keys_returns_all_keys(self, simple_cache):
        assert simple_cache.keys() == []

        simple_cache.set_bucket("key1", SimpleSchema())
        simple_cache.set_bucket("key2", SimpleSchema())
        simple_cache.set_bucket("key3", SimpleSchema())

        keys = simple_cache.keys()
        assert len(keys) == 3
        assert "key1" in keys
        assert "key2" in keys
        assert "key3" in keys


class TestMutability:
    def test_get_returns_same_instance(self, simple_cache):
        bucket_data = SimpleSchema(field1="original", field2=42)
        simple_cache.set_bucket("key1", bucket_data)

        result1 = simple_cache.get_bucket("key1")
        result1.field1 = "modified"

        result2 = simple_cache.get_bucket("key1")
        assert result2.field1 == "modified"

    def test_set_stores_same_instance(self, simple_cache):
        bucket_data = SimpleSchema(field1="original")
        simple_cache.set_bucket("key1", bucket_data)

        bucket_data.field1 = "modified_after_set"

        stored = simple_cache.get_bucket("key1")
        assert stored.field1 == "modified_after_set"


class TestTypeValidation:
    def test_set_bucket_accepts_correct_type(self, simple_cache):
        bucket_data = SimpleSchema(field1="test")
        simple_cache.set_bucket("key1", bucket_data)

        result = simple_cache.get_bucket("key1")
        assert result is not None
        assert result.field1 == "test"

    def test_set_bucket_rejects_wrong_type(self, simple_cache):
        with pytest.raises(TypeError, match="Expected SimpleSchema, got AnotherSchema"):
            simple_cache.set_bucket("key1", AnotherSchema(name="test"))

    def test_pydantic_instance_accepted(self):
        from pydantic import BaseModel

        class PydanticSchema(BaseModel):
            name: str
            value: int = 0

        cache = SimpleCache(PydanticSchema)

        try:
            cache.set_bucket("key1", PydanticSchema(name="test", value=42))
            result = cache.get_bucket("key1")
            assert result is not None
            assert result.name == "test"
            assert result.value == 42
        finally:
            cache.clear()

    def test_different_caches_different_types(self):
        cache1 = SimpleCache(SimpleSchema)
        cache2 = SimpleCache(AnotherSchema)

        try:
            cache1.set_bucket("key1", SimpleSchema(field1="test"))
            cache2.set_bucket("key1", AnotherSchema(name="test", value=1.5))

            result1 = cache1.get_bucket("key1")
            result2 = cache2.get_bucket("key1")

            assert result1 is not None
            assert result1.field1 == "test"

            assert result2 is not None
            assert result2.name == "test"
            assert result2.value == 1.5
        finally:
            cache1.clear()
            cache2.clear()


class TestConcurrency:
    def test_validate_race_condition(self, simple_cache):
        """
        Demonstrates a logical race condition (Lost Update) that happens
        when a check-then-act pattern isn't protected by a lock.
        Highly vulnerable in no-GIL environments.
        """
        n_workers = 10
        barrier = threading.Barrier(n_workers, timeout=3)

        init_count = 0
        target_key = "shared_write_once_key"

        def initialize_if_missing(thread_id: int):
            nonlocal init_count
            barrier.wait()
            if simple_cache.get_bucket(target_key) is None:
                # Force a tiny context switch window to reliably trigger the race
                time.sleep(0.005)
                init_count += 1
                simple_cache.set_bucket(
                    target_key,
                    SimpleSchema(
                        field1=f"initialized_by_{thread_id}", field2=thread_id
                    ),
                )

        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = [executor.submit(initialize_if_missing, i) for i in range(10)]
            for f in futures:
                f.result()

        assert init_count > 1

    def test_race_condition(self, simple_cache):

        n_workers = 10
        barrier = threading.Barrier(n_workers, timeout=3)

        init_count = 0
        target_key = "shared_write_once_key"

        def initialize_if_missing(thread_id: int):
            nonlocal init_count
            barrier.wait()
            with simple_cache.lock:
                if simple_cache.get_bucket(target_key) is None:
                    time.sleep(0.005)
                    init_count += 1
                    simple_cache.set_bucket(
                        target_key,
                        SimpleSchema(
                            field1=f"initialized_by_{thread_id}", field2=thread_id
                        ),
                    )

        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = [executor.submit(initialize_if_missing, i) for i in range(10)]
            for f in futures:
                f.result()

        assert init_count == 1, "Race condition occurred"
