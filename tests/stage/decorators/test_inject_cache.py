from dataclasses import dataclass
from typing import Any

import pytest

from tadween_core.broker import Message
from tadween_core.stage.decorators import inject_cache


@pytest.fixture(scope="module")
def cache():
    bucket = FakeBucket(audio_array="cached_audio_data")
    cache = FakeCache(buckets={"bucket_1": bucket})
    return cache


@dataclass
class FakeBucket:
    audio_array: str | None = None
    metadata: dict | None = None


class FakeCache:
    def __init__(self, buckets: dict[str, FakeBucket]):
        self.buckets = buckets

    def get_bucket(self, key: str) -> FakeBucket | None:
        return self.buckets.get(key)


class FakePolicy:
    @inject_cache(cache_field="audio_array", inject_as="audio")
    def resolve_inputs(self, message: Message, audio: str | None = None, **kwargs: Any):
        return {"audio": audio, "kwargs": kwargs}


def test_inject_cache_happy_path(cache):
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"cache_key": "bucket_1"})

    result = policy.resolve_inputs(msg, cache=cache)

    assert result["audio"] == "cached_audio_data"


def test_inject_cache_no_cache():
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"cache_key": "bucket_1"})

    # Should not raise and audio should remain None
    result = policy.resolve_inputs(msg, cache=None)

    assert result["audio"] is None


def test_inject_cache_missing_key(cache):

    policy = FakePolicy()
    msg = Message(topic="test", metadata={"other_key": "bucket_1"})

    result = policy.resolve_inputs(msg, cache=cache)

    assert result["audio"] is None


def test_inject_cache_missing_bucket():
    cache = FakeCache(buckets={})
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"cache_key": "missing"})

    result = policy.resolve_inputs(msg, cache=cache)

    assert result["audio"] is None


def test_inject_cache_already_injected(cache):
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"cache_key": "bucket_1"})

    # If it's already in kwargs (e.g. from an outer decorator), it should be preserved
    result = policy.resolve_inputs(msg, audio="pre-existing", cache=cache)

    assert result["audio"] == "pre-existing"


def test_inject_cache_missing_field_in_bucket():
    @dataclass
    class OtherBucket:
        something_else: str = "val"

    cache = FakeCache(buckets={"bucket_1": OtherBucket()})
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"cache_key": "bucket_1"})

    # Field 'audio_array' doesn't exist on OtherBucket
    result = policy.resolve_inputs(msg, cache=cache)

    assert result["audio"] is None


def test_inject_cache_custom_key():
    class CustomPolicy:
        @inject_cache(cache_field="audio_array", inject_as="audio", cache_key="my_id")
        def resolve_inputs(
            self, message: Message, audio: str | None = None, **kwargs: Any
        ):
            return audio

    bucket = FakeBucket(audio_array="custom_data")
    cache = FakeCache(buckets={"custom_123": bucket})
    policy = CustomPolicy()
    msg = Message(topic="test", metadata={"my_id": "custom_123"})

    result = policy.resolve_inputs(msg, cache=cache)
    assert result == "custom_data"
