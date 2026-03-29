from typing import Any

import pytest
from pydantic import BaseModel

from tadween_core.broker import Message
from tadween_core.exceptions import (
    InputValidationError,
    PolicyError,
)
from tadween_core.handler.base import BaseHandler
from tadween_core.stage.policy import DefaultStagePolicy
from tadween_core.stage.stage import Stage


class MyInput(BaseModel):
    value: int


class MyOutput(BaseModel):
    result: str


class SuccessHandler(BaseHandler[MyInput, MyOutput]):
    def run(self, inputs: MyInput) -> MyOutput:
        return MyOutput(result=str(inputs.value))


class FailureHandler(BaseHandler[MyInput, MyOutput]):
    def run(self, inputs: MyInput) -> MyOutput:
        raise ValueError("Handler explosion!")


class FailingResolvePolicy(DefaultStagePolicy[MyInput, MyOutput, Any]):
    def resolve_inputs(self, message, repo=None, cache=None):
        raise RuntimeError("Policy resolution failed!")


class FailingSuccessPolicy(DefaultStagePolicy[MyInput, MyOutput, Any]):
    def on_success(self, task_id, message, result, broker=None, repo=None, cache=None):
        raise RuntimeError("Policy on_success failed!")


def test_policy_resolve_error():
    handler = SuccessHandler()
    stage = Stage(handler=handler, name="TestStage", policy=FailingResolvePolicy())

    msg = Message(topic="test", payload={"value": 1})

    try:
        with pytest.raises(PolicyError) as excinfo:
            stage.submit_message(msg)

        assert "Policy resolution failed!" in str(excinfo.value)
        assert "stage_name=TestStage" in str(excinfo.value)
        assert "policy_name=FailingResolvePolicy" in str(excinfo.value)
        assert "method=resolve_inputs" in str(excinfo.value)
    finally:
        stage.close()


def test_input_validation_error():
    handler = SuccessHandler()
    stage = Stage(handler=handler, name="ValidationStage")

    msg = Message(topic="test", payload={"wrong_key": 1})

    try:
        with pytest.raises(InputValidationError) as excinfo:
            stage.submit_message(msg)

        assert "Pydantic validation failed" in str(excinfo.value)
        assert "stage_name=ValidationStage" in str(excinfo.value)
    finally:
        stage.close()


def test_handler_error():
    class CapturePolicy(DefaultStagePolicy):
        def __init__(self):
            self.last_error = None

        def on_error(self, message, error, broker=None):
            self.last_error = error

    policy = CapturePolicy()
    handler = FailureHandler()
    stage = Stage(handler=handler, name="ExplodingStage", policy=policy)

    msg = Message(topic="test", payload={"value": 1})

    try:
        stage.submit_message(msg)
        stage.task_queue.wait_all()

        assert "Handler explosion!" in str(policy.last_error)
        assert "stage_name=ExplodingStage" in str(policy.last_error)
        assert policy.last_error.context["task_id"] is not None
    finally:
        stage.close()


def test_policy_on_success_error():
    class CapturePolicy(FailingSuccessPolicy):
        def __init__(self):
            self.last_error = None

        def on_error(self, message, error, broker=None):
            self.last_error = error

    policy = CapturePolicy()
    handler = SuccessHandler()
    stage = Stage(handler=handler, name="SuccessPolicyStage", policy=policy)

    msg = Message(topic="test", payload={"value": 1})

    try:
        stage.submit_message(msg)
        stage.task_queue.wait_all()

        assert isinstance(policy.last_error, PolicyError)
        assert "Policy on_success failed!" in str(policy.last_error)
        assert "stage_name=SuccessPolicyStage" in str(policy.last_error)
        assert "method=on_success" in str(policy.last_error)
    finally:
        stage.close()
