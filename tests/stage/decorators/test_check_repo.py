from tadween_core.broker import Message
from tadween_core.stage.decorators import check_repo
from tadween_core.stage.policy import InterceptionContext


class FakeArtifact:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class FakeRepo:
    def __init__(self, artifacts, valid_parts):
        self.artifacts = artifacts
        self._part_map = dict.fromkeys(valid_parts)

    def load(self, artifact_id, include=None, **kwargs):
        if artifact_id not in self.artifacts:
            return None
        return self.artifacts[artifact_id]


class FakePolicy:
    @check_repo(
        aid="artifact_id", condition="audio | transcript & (meta.is_valid | valid)"
    )
    def intercept(self, message, broker=None, repo=None, cache=None):
        return InterceptionContext(intercepted=False, reason="proceed")

    @check_repo(aid="artifact_id", condition="diarization.speakers")
    def intercept_simple(self, message, broker=None, repo=None, cache=None):
        return InterceptionContext(intercepted=False, reason="proceed")


def test_check_repo_no_repo():
    policy = FakePolicy()
    msg = Message(topic="test", payload={}, metadata={"artifact_id": "123"})
    result = policy.intercept(msg, repo=None)
    assert not result.intercepted
    assert result.reason == "proceed"


def test_check_repo_no_artifact_id():
    policy = FakePolicy()
    repo = FakeRepo(artifacts={}, valid_parts=["audio"])
    msg = Message(topic="test", payload={}, metadata={"other_id": "123"})
    result = policy.intercept(msg, repo=repo)
    assert not result.intercepted
    assert result.reason == "proceed"


def test_check_repo_artifact_not_found():
    policy = FakePolicy()
    repo = FakeRepo(artifacts={}, valid_parts=["audio", "transcript", "meta", "valid"])
    msg = Message(topic="test", payload={}, metadata={"artifact_id": "missing"})
    result = policy.intercept(msg, repo=repo)
    assert not result.intercepted
    assert result.reason == "proceed"


def test_check_repo_condition_met_simple():
    policy = FakePolicy()
    artifact = FakeArtifact(diarization=FakeArtifact(speakers=["spk1", "spk2"]))
    repo = FakeRepo(artifacts={"123": artifact}, valid_parts=["diarization"])
    msg = Message(topic="test", payload={}, metadata={"artifact_id": "123"})

    result = policy.intercept_simple(msg, repo=repo)
    assert result.intercepted


def test_check_repo_condition_not_met_simple():
    policy = FakePolicy()
    # Missing speakers field
    artifact = FakeArtifact(diarization=FakeArtifact())
    repo = FakeRepo(artifacts={"123": artifact}, valid_parts=["diarization"])
    msg = Message(topic="test", payload={}, metadata={"artifact_id": "123"})

    result = policy.intercept_simple(msg, repo=repo)
    assert not result.intercepted


def test_check_repo_condition_met_complex():
    policy = FakePolicy()
    # Condition: audio | transcript & (meta.is_valid | valid)
    # Let's make transcript=True, valid=True, audio=False
    artifact = FakeArtifact(transcript="some_text", valid=True)
    repo = FakeRepo(
        artifacts={"123": artifact},
        valid_parts=["audio", "transcript", "meta", "valid"],
    )
    msg = Message(topic="test", payload={}, metadata={"artifact_id": "123"})

    result = policy.intercept(msg, repo=repo)
    assert result.intercepted


def test_check_repo_condition_not_met_complex():
    policy = FakePolicy()
    # audio=False, transcript=True, but meta.is_valid=False and valid=False
    artifact = FakeArtifact(transcript="some_text")
    repo = FakeRepo(
        artifacts={"123": artifact},
        valid_parts=["audio", "transcript", "meta", "valid"],
    )
    msg = Message(topic="test", payload={}, metadata={"artifact_id": "123"})

    result = policy.intercept(msg, repo=repo)
    assert not result.intercepted
