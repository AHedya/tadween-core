import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest

from tadween_core.broker import Message
from tadween_core.cache import Cache
from tadween_core.cache.simple_cache import SimpleCache
from tadween_core.stage.decorators import write_cache


@dataclass
class ResultSchema:
    audio_array: str | None = None
    metadata: dict | None = None
    value: int | None = None


@dataclass
class BucketSchema:
    cached_audio: str | None = None
    cached_meta: dict | None = None
    cached_value: int | None = None
    cached_result: ResultSchema | None = None


@pytest.fixture(params=["simple", "proxy"])
def cache(request):
    if request.param == "simple":
        c = SimpleCache(BucketSchema)
    else:
        c = Cache(BucketSchema)
    yield c
    c.clear()


class FakePolicy:
    def __init__(self):
        self.calls = []

    @write_cache(cache_field="cached_audio", result_field="audio_array")
    def on_success_before(
        self, task_id, message, result, broker=None, repo=None, cache=None
    ):
        self.calls.append(("method", task_id, message, result))

    @write_cache(cache_field="cached_audio", result_field="audio_array", mode="after")
    def on_success_after(
        self, task_id, message, result, broker=None, repo=None, cache=None
    ):
        self.calls.append(("method", task_id, message, result))


def test_raises_value_error_on_length_mismatch():
    with pytest.raises(ValueError, match="Length mismatch"):
        write_cache(
            cache_field=["f1", "f2", "f3"],
            result_field=["r1", "r2"],
        )


class TestWriteCache:
    def test_single_field_mapping(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data_123")
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        policy.on_success_before("task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_1")
        assert bucket is not None
        assert bucket.cached_audio == "audio_data_123"
        assert policy.calls[0] == ("method", "task-1", message, result)

    def test_single_field_with_none_result_value(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array=None)
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        cache.set_bucket("bucket_1", BucketSchema(cached_audio="existing"))

        policy.on_success_before("task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_1")
        assert bucket.cached_audio == "existing"

    def test_single_field_whole_result_object(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio", metadata={"k": "v"}, value=42)

        @write_cache(cache_field="cached_result", result_field=None)
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        on_success(policy, "task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_1")
        assert bucket is not None
        assert bucket.cached_result == result

    def test_single_field_missing_result_attribute(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio")

        @write_cache(cache_field="cached_meta", result_field="metadata")
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        cache.set_bucket("bucket_1", BucketSchema(cached_meta={"existing": "data"}))
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        on_success(policy, "task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_1")
        assert bucket.cached_meta == {"existing": "data"}

    def test_multiple_field_mapping(self, cache):
        policy = FakePolicy()
        result = ResultSchema(
            audio_array="audio_data", metadata={"format": "mp3"}, value=42
        )

        @write_cache(
            cache_field=["cached_audio", "cached_meta"],
            result_field=["audio_array", "metadata"],
        )
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        on_success(policy, "task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_1")
        assert bucket is not None
        assert bucket.cached_audio == "audio_data"
        assert bucket.cached_meta == {"format": "mp3"}

    def test_multiple_fields_mixed_none_values(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data", metadata=None, value=42)

        @write_cache(
            cache_field=["cached_audio", "cached_meta"],
            result_field=["audio_array", "metadata"],
        )
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        cache.set_bucket("bucket_1", BucketSchema(cached_meta={"existing": "data"}))
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        on_success(policy, "task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_1")
        assert bucket.cached_audio == "audio_data"
        assert bucket.cached_meta == {"existing": "data"}

    def test_creates_new_bucket_if_not_exists(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata={"cache_key": "new_bucket"})

        assert cache.get_bucket("new_bucket") is None

        policy.on_success_before("task-1", message, result, cache=cache)

        bucket = cache.get_bucket("new_bucket")
        assert bucket is not None
        assert bucket.cached_audio == "audio_data"

    def test_writes_to_existing_bucket(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="new_audio")
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        cache.set_bucket("bucket_1", BucketSchema(cached_audio="old_audio"))

        policy.on_success_before("task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_1")
        assert bucket.cached_audio == "new_audio"

    def test_thread_safe_bucket_creation(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata={"cache_key": "shared_bucket"})

        n_workers = 10
        barrier = threading.Barrier(n_workers, timeout=3)

        def run_decorator(thread_id: int):
            barrier.wait()
            on_success = write_cache(
                cache_field="cached_audio", result_field="audio_array"
            )(
                lambda self, task_id, message, result, broker=None, repo=None, cache=None: (
                    None
                )
            )
            on_success(policy, f"task-{thread_id}", message, result, cache=cache)

        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = [executor.submit(run_decorator, i) for i in range(n_workers)]
            for f in futures:
                f.result()

        bucket = cache.get_bucket("shared_bucket")
        assert bucket is not None
        assert bucket.cached_audio == "audio_data"
        assert len(cache) == 1

    def test_skips_write_when_cache_is_none(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        policy.on_success_before("task-1", message, result, cache=None)

        assert cache.get_bucket("bucket_1") is None
        assert len(policy.calls) == 1

    def test_skips_write_when_bucket_key_missing(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata={"other_key": "value"})

        policy.on_success_before("task-1", message, result, cache=cache)

        assert cache.get_bucket("cache_key") is None

    def test_skips_write_when_metadata_is_none(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata=None)

        policy.on_success_before("task-1", message, result, cache=cache)

        assert len(cache) == 0

    def test_mode_before_writes_before_method(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        policy.on_success_before("task-1", message, result, cache=cache)

        assert len(policy.calls) == 1
        assert policy.calls[0][0] == "method"

        bucket = cache.get_bucket("bucket_1")
        assert bucket.cached_audio == "audio_data"

    def test_mode_after_writes_after_method(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        policy.on_success_after("task-1", message, result, cache=cache)

        assert len(policy.calls) == 1

        bucket = cache.get_bucket("bucket_1")
        assert bucket.cached_audio == "audio_data"

    def test_custom_cache_key_from_metadata(self, cache):
        policy = FakePolicy()
        result = ResultSchema(audio_array="audio_data")
        message = Message(topic="test", metadata={"my_custom_key": "bucket_42"})

        @write_cache(
            cache_field="cached_audio",
            result_field="audio_array",
            cache_key="my_custom_key",
        )
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        on_success(policy, "task-1", message, result, cache=cache)

        bucket = cache.get_bucket("bucket_42")
        assert bucket is not None
        assert bucket.cached_audio == "audio_data"


@dataclass
class RequiredFieldBucket:
    required_field: str
    optional_field: str | None = None


@dataclass
class RequiredFieldResult:
    required_value: str
    optional_value: str | None = None


@pytest.fixture(params=["simple", "proxy"])
def required_field_cache(request):
    if request.param == "simple":
        c = SimpleCache(RequiredFieldBucket)
    else:
        c = Cache(RequiredFieldBucket)
    yield c
    c.clear()


class TestWriteCacheRequiredFields:
    def test_happy_path_required_field_provided(self, required_field_cache):
        policy = FakePolicy()

        @write_cache(
            cache_field=["required_field", "optional_field"],
            result_field=["required_value", "optional_value"],
        )
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        result = RequiredFieldResult(
            required_value="required_data", optional_value="optional_data"
        )
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        on_success(policy, "task-1", message, result, cache=required_field_cache)

        bucket = required_field_cache.get_bucket("bucket_1")
        assert bucket is not None
        assert bucket.required_field == "required_data"
        assert bucket.optional_field == "optional_data"

    def test_happy_path_only_required_field(self, required_field_cache):
        policy = FakePolicy()

        @write_cache(cache_field="required_field", result_field="required_value")
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        result = RequiredFieldResult(required_value="required_data")
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        on_success(policy, "task-1", message, result, cache=required_field_cache)

        bucket = required_field_cache.get_bucket("bucket_1")
        assert bucket is not None
        assert bucket.required_field == "required_data"
        assert bucket.optional_field is None

    def test_not_happy_path_missing_required_field(self, required_field_cache):
        policy = FakePolicy()

        @write_cache(cache_field="optional_field", result_field="optional_value")
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        result = RequiredFieldResult(
            required_value="ignored", optional_value="optional_data"
        )
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        with pytest.raises(TypeError, match="required_field"):
            on_success(policy, "task-1", message, result, cache=required_field_cache)

    def test_not_happy_path_none_required_field(self, required_field_cache):
        policy = FakePolicy()

        @write_cache(cache_field="required_field", result_field="required_value")
        def on_success(
            self, task_id, message, result, broker=None, repo=None, cache=None
        ):
            pass

        result = RequiredFieldResult(required_value=None)
        message = Message(topic="test", metadata={"cache_key": "bucket_1"})

        with pytest.raises(TypeError, match="required_field"):
            on_success(policy, "task-1", message, result, cache=required_field_cache)
