from unittest.mock import MagicMock

import pytest

from tadween_core.broker import Message
from tadween_core.exceptions import PolicyError, RoutingError
from tadween_core.stage.policy import InterceptionAction, InterceptionContext
from tadween_core.task_queue.base import TaskEnvelope, TaskMetadata
from tadween_core.workflow.router import WorkflowRoutingPolicy


class TestWorkflowRoutingPolicy:
    def test_resolve_inputs_delegates(self):
        inner_policy = MagicMock()
        inner_policy.resolve_inputs.return_value = {"res": 1}
        router = WorkflowRoutingPolicy(stage_policy=inner_policy, output_topics=[])
        msg = Message(topic="in")

        res = router.resolve_inputs(msg)
        assert res == {"res": 1}
        inner_policy.resolve_inputs.assert_called_once_with(
            message=msg, repo=None, cache=None
        )

    def test_intercept_false_delegates(self):
        inner_policy = MagicMock()
        inner_policy.intercept.return_value = InterceptionContext(intercepted=False)
        router = WorkflowRoutingPolicy(stage_policy=inner_policy, output_topics=[])
        msg = Message(topic="in")

        ctx = router.intercept(msg)
        assert ctx.intercepted is False
        inner_policy.intercept.assert_called_once_with(
            message=msg, broker=None, repo=None, cache=None
        )

    def test_intercept_true_routes_and_acks(self):
        inner_policy = MagicMock()
        inner_policy.intercept.return_value = InterceptionContext(
            intercepted=True, payload={"cached": True}, reason="cache"
        )
        broker = MagicMock()
        router = WorkflowRoutingPolicy(
            stage_policy=inner_policy,
            output_topics=["out.topic"],
            broker=broker,
            payload_extractor=lambda x: x,
        )
        msg = Message(topic="in", payload={"data": 1})

        ctx = router.intercept(msg)
        assert ctx.intercepted is True

        inner_policy.on_done.assert_called_once()
        inner_policy.on_success.assert_called_once_with(
            task_id="N/A",
            message=msg,
            result={"cached": True},
            broker=broker,
            repo=None,
            cache=None,
        )

        broker.publish.assert_called_once()
        pub_msg = broker.publish.call_args[0][0]
        assert pub_msg.topic == "out.topic"
        assert pub_msg.payload == {"cached": True}

        broker.ack.assert_called_once_with(msg.id)

    def test_intercept_cancel_preset(self):
        inner_policy = MagicMock()

        inner_policy.intercept.return_value = InterceptionContext(
            intercepted=True, action=InterceptionAction.cancel()
        )
        broker = MagicMock()
        router = WorkflowRoutingPolicy(
            stage_policy=inner_policy,
            output_topics=["out.topic"],
            broker=broker,
        )
        msg = Message(topic="in")

        router.intercept(msg)

        inner_policy.on_done.assert_not_called()
        inner_policy.on_success.assert_not_called()
        broker.publish.assert_not_called()
        broker.ack.assert_called_once_with(msg.id)

    def test_intercept_custom_action(self):
        inner_policy = MagicMock()

        # Custom: only ack and on_done, no on_success, no publish
        inner_policy.intercept.return_value = InterceptionContext(
            intercepted=True,
            action=InterceptionAction(
                on_done=True, on_success=False, publish=False, ack=True
            ),
        )
        broker = MagicMock()
        router = WorkflowRoutingPolicy(
            stage_policy=inner_policy,
            output_topics=["out.topic"],
            broker=broker,
        )
        msg = Message(topic="in")

        router.intercept(msg)

        inner_policy.on_done.assert_called_once()
        inner_policy.on_success.assert_not_called()
        broker.publish.assert_not_called()
        broker.ack.assert_called_once_with(msg.id)

    def test_intercept_payload_extractor_failure(self):
        inner_policy = MagicMock()
        inner_policy.intercept.return_value = InterceptionContext(
            intercepted=True, payload={"cached": True}
        )
        broker = MagicMock()

        def bad_extractor(p):  # noqa: ARG001
            raise ValueError("bad extractor")

        router = WorkflowRoutingPolicy(
            stage_policy=inner_policy,
            output_topics=["out"],
            broker=broker,
            payload_extractor=bad_extractor,
        )
        msg = Message(topic="in")

        with pytest.raises(PolicyError):
            router.intercept(msg)

    def test_intercept_routing_error(self):
        inner_policy = MagicMock()
        inner_policy.intercept.return_value = InterceptionContext(
            intercepted=True, payload={"cached": True}
        )
        broker = MagicMock()
        broker.publish.side_effect = Exception("broker down")

        router = WorkflowRoutingPolicy(
            stage_policy=inner_policy, output_topics=["out"], broker=broker
        )
        msg = Message(topic="in")

        with pytest.raises(RoutingError):
            router.intercept(msg)

    def test_on_success_routes_and_acks(self):
        inner_policy = MagicMock()
        broker = MagicMock()

        router = WorkflowRoutingPolicy(
            stage_policy=inner_policy,
            output_topics=["next"],
            broker=broker,
            payload_extractor=lambda x: x,
        )
        msg = Message(topic="in")

        router.on_success("t1", msg, {"out": 42})

        inner_policy.on_success.assert_called_once_with(
            task_id="t1",
            message=msg,
            result={"out": 42},
            broker=broker,
            repo=None,
            cache=None,
        )

        broker.publish.assert_called_once()
        pub_msg = broker.publish.call_args[0][0]
        assert pub_msg.topic == "next"
        assert pub_msg.payload == {"out": 42}

        broker.ack.assert_called_once_with(msg.id)

    def test_on_error_delegates_and_nack(self):
        inner_policy = MagicMock()
        broker = MagicMock()

        router = WorkflowRoutingPolicy(
            stage_policy=inner_policy, output_topics=[], broker=broker
        )
        msg = Message(topic="in")
        err = ValueError("err")

        router.on_error(msg, err)

        inner_policy.on_error.assert_called_once_with(
            message=msg, error=err, broker=broker
        )
        broker.nack.assert_called_once_with(msg.id, requeue_message=None)

    def test_on_done_delegates(self):
        inner_policy = MagicMock()
        router = WorkflowRoutingPolicy(stage_policy=inner_policy, output_topics=[])
        msg = Message(topic="in")
        env = TaskEnvelope(
            payload=None,
            metadata=TaskMetadata(
                task_id="t1", start_time=0, end_time=0, submit_time=0
            ),
            error=None,
            success=True,
        )

        router.on_done(msg, env)
        inner_policy.on_done.assert_called_once_with(message=msg, envelope=env)

    def test_on_running_delegates(self):
        inner_policy = MagicMock()
        router = WorkflowRoutingPolicy(stage_policy=inner_policy, output_topics=[])
        msg = Message(topic="in")

        router.on_running("t1", msg)
        inner_policy.on_running.assert_called_once_with(task_id="t1", message=msg)
