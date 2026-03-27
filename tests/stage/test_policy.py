from typing import Any

from tadween_core.broker import Message
from tadween_core.stage.policy import (
    DefaultStagePolicy,
    InterceptionContext,
    StagePolicyBuilder,
)
from tadween_core.task_queue.base import TaskEnvelope, TaskMetadata


class TestInterceptionContext:
    def test_interception_context_intercepted_true(self):
        ctx = InterceptionContext(
            intercepted=True, payload={"result": "cached"}, reason="cache_hit"
        )

        assert ctx.intercepted is True
        assert ctx.payload == {"result": "cached"}
        assert ctx.reason == "cache_hit"

    def test_interception_context_intercepted_false(self):
        ctx = InterceptionContext(intercepted=False)

        assert ctx.intercepted is False
        assert ctx.payload is None
        assert ctx.reason is None

    def test_interception_context_default_values(self):
        ctx = InterceptionContext(intercepted=True)

        assert ctx.intercepted is True
        assert ctx.payload is None
        assert ctx.reason is None


class TestDefaultStagePolicy:
    def test_resolve_inputs_returns_payload(self):
        policy = DefaultStagePolicy[Any, Any, Any, Any, Any]()
        msg = Message(topic="test", payload={"key": "value"})

        result = policy.resolve_inputs(msg)

        assert result == {"key": "value"}

    def test_resolve_inputs_returns_empty_dict_if_no_payload(self):
        policy = DefaultStagePolicy[Any, Any, Any, Any, Any]()
        msg = Message(topic="test")

        result = policy.resolve_inputs(msg)

        assert result == {}

    def test_intercept_returns_false(self):
        policy = DefaultStagePolicy[Any, Any, Any, Any, Any]()
        msg = Message(topic="test", payload={"data": 123})

        ctx = policy.intercept(msg)

        assert ctx.intercepted is False
        assert ctx.payload is None

    def test_on_running_no_op(self):
        policy = DefaultStagePolicy[Any, Any, Any, Any, Any]()
        msg = Message(topic="test")

        policy.on_running("task-123", msg)

    def test_on_done_no_op(self):
        policy = DefaultStagePolicy[Any, Any, Any, Any, Any]()
        msg = Message(topic="test")
        envelope = TaskEnvelope(
            payload=None,
            metadata=TaskMetadata(
                task_id="task-123", start_time=0.0, end_time=1.0, submit_time=0.0
            ),
            error=None,
            success=True,
        )

        policy.on_done(msg, envelope)

    def test_on_success_no_op(self):
        policy = DefaultStagePolicy[Any, Any, Any, Any, Any]()
        msg = Message(topic="test")

        policy.on_success("task-123", msg, {"result": "done"})

    def test_on_error_no_op(self):
        policy = DefaultStagePolicy[Any, Any, Any, Any, Any]()
        msg = Message(topic="test")
        error = ValueError("test error")

        policy.on_error(msg, error)


class TestStagePolicyBuilder:
    def test_builder_returns_default_behavior_without_overrides(self):
        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]()
        msg = Message(topic="test", payload={"data": 42})

        assert policy.resolve_inputs(msg) == {"data": 42}
        assert policy.intercept(msg).intercepted is False

    def test_builder_with_resolve_inputs(self):
        custom_inputs = {"custom": "value"}

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_resolve_inputs(
            lambda msg, repo=None, cache=None: custom_inputs
        )

        msg = Message(topic="test", payload={"original": "data"})
        result = policy.resolve_inputs(msg)

        assert result == custom_inputs

    def test_builder_with_intercept(self):
        custom_payload = {"result": "cached_result"}

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_intercept(
            lambda msg, broker=None, repo=None, cache=None: InterceptionContext(
                intercepted=True, payload=custom_payload, reason="cache_hit"
            )
        )

        msg = Message(topic="test", payload={"data": 123})
        ctx = policy.intercept(msg)

        assert ctx.intercepted is True
        assert ctx.payload == custom_payload
        assert ctx.reason == "cache_hit"

    def test_builder_with_on_running(self):
        calls = []

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_on_running(
            lambda task_id, msg: calls.append(("running", task_id))
        )

        msg = Message(topic="test")
        policy.on_running("task-123", msg)

        assert len(calls) == 1
        assert calls[0] == ("running", "task-123")

    def test_builder_with_on_done(self):
        calls = []

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_on_done(
            lambda msg, envelope: calls.append(("done", envelope.success))
        )

        msg = Message(topic="test")
        envelope = TaskEnvelope(
            payload={"result": "ok"},
            metadata=TaskMetadata(
                task_id="task-123", start_time=0.0, end_time=1.0, submit_time=0.0
            ),
            error=None,
            success=True,
        )
        policy.on_done(msg, envelope)

        assert len(calls) == 1
        assert calls[0] == ("done", True)

    def test_builder_with_on_success(self):
        calls = []

        def on_success_cb(task_id, message, result, broker=None, repo=None, cache=None):  # noqa: ARG001
            calls.append((task_id, result))

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_on_success(
            on_success_cb
        )

        msg = Message(topic="test")
        policy.on_success("task-456", msg, {"result": "success"})

        assert len(calls) == 1
        assert calls[0] == ("task-456", {"result": "success"})

    def test_builder_with_on_error(self):
        calls = []

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_on_error(
            lambda msg, err, broker=None: calls.append(("error", str(err)))
        )

        msg = Message(topic="test")
        error = RuntimeError("something went wrong")
        policy.on_error(msg, error)

        assert len(calls) == 1
        assert "something went wrong" in calls[0][1]

    def test_builder_chaining(self):
        calls = []

        def on_success_cb(task_id, message, result, broker=None, repo=None, cache=None):  # noqa: ARG001
            calls.append("success")

        policy = (
            StagePolicyBuilder[Any, Any, Any, Any, Any]()
            .with_resolve_inputs(lambda msg, repo=None, cache=None: {"resolved": True})
            .with_intercept(
                lambda msg, broker=None, repo=None, cache=None: InterceptionContext(
                    intercepted=False
                )
            )
            .with_on_running(lambda tid, msg: calls.append("running"))
            .with_on_done(lambda msg, env: calls.append("done"))
            .with_on_success(on_success_cb)
            .with_on_error(lambda msg, err, broker=None: calls.append("error"))
        )

        msg = Message(topic="test")
        envelope = TaskEnvelope(
            payload=None,
            metadata=TaskMetadata(
                task_id="t1", start_time=0.0, end_time=1.0, submit_time=0.0
            ),
            error=None,
            success=True,
        )

        assert policy.resolve_inputs(msg) == {"resolved": True}
        assert policy.intercept(msg).intercepted is False
        policy.on_running("t1", msg)
        policy.on_done(msg, envelope)
        policy.on_success("t1", msg, {"result": "ok"})
        policy.on_error(msg, ValueError("err"))

        assert len(calls) == 4

    def test_builder_with_on_success_receives_broker_repo_cache(self):
        received = {}

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_on_success(
            lambda task_id, msg, result, broker, repo, cache: received.update(
                {"broker": broker, "repo": repo, "cache": cache, "result": result}
            )
        )

        msg = Message(topic="test")
        fake_broker = object()
        fake_repo = object()
        fake_cache = object()

        policy.on_success(
            "task-789",
            msg,
            {"data": "test"},
            broker=fake_broker,
            repo=fake_repo,
            cache=fake_cache,
        )

        assert received["broker"] is fake_broker
        assert received["repo"] is fake_repo
        assert received["cache"] is fake_cache
        assert received["result"] == {"data": "test"}

    def test_builder_intercept_with_repo_and_cache(self):
        received = {}

        policy = StagePolicyBuilder[Any, Any, Any, Any, Any]().with_intercept(
            lambda msg, broker, repo, cache: (
                received.update({"repo": repo, "cache": cache}),
                InterceptionContext(intercepted=False),
            )[1]
        )

        msg = Message(topic="test")
        fake_repo = object()
        fake_cache = object()

        ctx = policy.intercept(msg, repo=fake_repo, cache=fake_cache)

        assert ctx.intercepted is False
        assert received["repo"] is fake_repo
        assert received["cache"] is fake_cache


class TestPolicyTypePreservation:
    def test_generic_types_preserved_in_builder(self):
        from pydantic import BaseModel

        class MyInput(BaseModel):
            x: int

        class MyOutput(BaseModel):
            y: str

        policy: StagePolicyBuilder[MyInput, MyOutput, dict, Any, str] = (
            StagePolicyBuilder()
        )

        msg = Message(topic="test", payload={"x": 10})
        result = policy.resolve_inputs(msg)

        assert result == {"x": 10}
