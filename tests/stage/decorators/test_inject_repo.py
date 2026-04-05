from typing import Any

import pytest

from tadween_core.broker import Message
from tadween_core.stage.decorators import inject_cache, inject_repo


class FakeRepo:
    def __init__(self, artifacts: dict[str, dict[str, Any]]):
        self.artifacts = artifacts

    def load_part(self, artifact_id: str, part_name: str) -> Any | None:
        if artifact_id not in self.artifacts:
            raise KeyError(f"Artifact {artifact_id} missing")
        return self.artifacts[artifact_id].get(part_name)


@pytest.fixture(scope="module")
def repo():
    repo = FakeRepo(artifacts={"art_1": {"audio": "repo_audio_data"}})
    return repo


class FakePolicy:
    @inject_repo(part="audio", inject_as="audio")
    def resolve_inputs(self, message: Message, audio: str | None = None, **kwargs: Any):
        return {"audio": audio, "kwargs": kwargs}


def test_inject_repo_happy_path(repo):
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"artifact_id": "art_1"})

    result = policy.resolve_inputs(msg, repo=repo)

    assert result["audio"] == "repo_audio_data"


def test_inject_repo_no_repo():
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"artifact_id": "art_1"})

    result = policy.resolve_inputs(msg, repo=None)

    assert result["audio"] is None


def test_inject_repo_missing_aid(repo):
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"other_id": "art_1"})

    result = policy.resolve_inputs(msg, repo=repo)

    assert result["audio"] is None


def test_inject_repo_missing_part():
    repo = FakeRepo(artifacts={"art_1": {"transcript": "text"}})
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"artifact_id": "art_1"})

    result = policy.resolve_inputs(msg, repo=repo)

    assert result["audio"] is None


def test_inject_repo_exception_handled():
    repo = FakeRepo(artifacts={})  # Artifact missing, should raise KeyError
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"artifact_id": "missing"})

    # Decorator should catch KeyError and proceed
    result = policy.resolve_inputs(msg, repo=repo)

    assert result["audio"] is None


def test_inject_repo_already_injected():
    repo = FakeRepo(artifacts={"art_1": {"audio": "repo"}})
    policy = FakePolicy()
    msg = Message(topic="test", metadata={"artifact_id": "art_1"})

    # audio provided directly (e.g. from outer decorator)
    result = policy.resolve_inputs(msg, audio="outer", repo=repo)

    assert result["audio"] == "outer"


@pytest.fixture
def fake_bucket():
    class Bucket:
        def __init__(self, audio_array):
            self.audio_array = audio_array

    return Bucket


def test_stacked_decorators_cache_hit(fake_bucket):
    class FakeCache:
        def get_bucket(self, key):
            return fake_bucket("cached_data")

    class CountingRepo:
        def __init__(self):
            self.calls = 0

        def load_part(self, aid, part):
            self.calls += 1
            return "repo_data"

    class StackedPolicy:
        @inject_cache(cache_field="audio_array", inject_as="audio")
        @inject_repo(part="audio", inject_as="audio")
        def resolve_inputs(self, message, audio=None, **kwargs):
            return audio

    cache = FakeCache()
    repo = CountingRepo()
    policy = StackedPolicy()
    msg = Message(topic="test", metadata={"cache_key": "k", "artifact_id": "a"})

    result = policy.resolve_inputs(msg, cache=cache, repo=repo)

    assert result == "cached_data"
    assert repo.calls == 0  # Repo should NOT be called


def test_stacked_decorators_cache_miss_repo_hit(fake_bucket):  # noqa: ARG001
    class FakeCache:
        def get_bucket(self, key):
            return None  # Miss

    class CountingRepo:
        def __init__(self):
            self.calls = 0

        def load_part(self, aid, part):
            self.calls += 1
            return "repo_data"

    class StackedPolicy:
        @inject_cache(cache_field="audio_array", inject_as="audio")
        @inject_repo(part="audio", inject_as="audio")
        def resolve_inputs(self, message, audio=None, **kwargs):
            return audio

    cache = FakeCache()
    repo = CountingRepo()
    policy = StackedPolicy()
    msg = Message(topic="test", metadata={"cache_key": "k", "artifact_id": "a"})

    result = policy.resolve_inputs(msg, cache=cache, repo=repo)

    assert result == "repo_data"
    assert repo.calls == 1
