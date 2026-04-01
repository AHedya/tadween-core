import time
from typing import Any

import pytest
from pydantic import BaseModel

from tadween_core.broker import Message
from tadween_core.exceptions import InputValidationError
from tadween_core.handler.base import BaseHandler
from tadween_core.stage.policy import (
    DefaultStagePolicy,
    InterceptionContext,
    StagePolicyBuilder,
)
from tadween_core.stage.stage import Stage


class InputModel(BaseModel):
    value: int


class OutputModel(BaseModel):
    result: str


class SuccessHandler(BaseHandler[InputModel, OutputModel]):
    def run(self, inputs: InputModel) -> OutputModel:
        return OutputModel(result=f"processed_{inputs.value}")


class SlowHandler(BaseHandler[InputModel, OutputModel]):
    def run(self, inputs: InputModel) -> OutputModel:
        time.sleep(0.1)
        return OutputModel(result=f"slow_{inputs.value}")


class FailingHandler(BaseHandler[InputModel, OutputModel]):
    def run(self, inputs: InputModel) -> OutputModel:
        raise ValueError("Handler failure!")


class WarmupHandler(BaseHandler[InputModel, OutputModel]):
    def __init__(self):
        self.warmed_up = False
        self.shutdown_called = False

    def warmup(self) -> None:
        self.warmed_up = True

    def run(self, inputs: InputModel) -> OutputModel:
        return OutputModel(result=f"warmed_{self.warmed_up}")

    def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.fixture
def success_stage():
    stage = Stage(handler=SuccessHandler(), name="SuccessStage")
    yield stage
    stage.close()


@pytest.fixture
def create_stage():
    stages: list[Stage] = []

    def _create(handler, name=None, policy=None):
        stage = Stage(handler=handler, name=name, policy=policy)
        stages.append(stage)
        return stage

    yield _create
    for s in stages:
        s.close()


class TestStageSubmission:
    def test_submit_message_returns_task_id(self, success_stage):
        msg = Message(topic="test", payload={"value": 42})
        task_id = success_stage.submit_message(msg)

        assert task_id is not None
        assert isinstance(task_id, str)
        success_stage.task_queue.wait_all()

    def test_submit_convenience_method(self, success_stage):
        input_data = InputModel(value=100)
        task_id = success_stage.submit(input_data)

        assert task_id is not None
        success_stage.task_queue.wait_all()

    def test_submit_with_dict_payload(self, success_stage):
        task_id = success_stage.submit({"value": 50})

        assert task_id is not None
        success_stage.task_queue.wait_all()

    def test_submit_with_metadata(self, success_stage):
        task_id = success_stage.submit({"value": 1}, metadata={"trace_id": "abc123"})

        assert task_id is not None
        success_stage.task_queue.wait_all()


class TestStageInputValidation:
    def test_valid_dict_input_converted(self, success_stage):
        msg = Message(topic="test", payload={"value": 10})
        task_id = success_stage.submit_message(msg)

        success_stage.task_queue.wait_all()
        assert task_id is not None

    def test_valid_model_input_passed_through(self, success_stage):
        msg = Message(topic="test", payload=InputModel(value=20))
        task_id = success_stage.submit_message(msg)

        success_stage.task_queue.wait_all()
        assert task_id is not None

    def test_invalid_input_missing_field_raises(self, success_stage):
        msg = Message(topic="test", payload={"wrong_key": 1})

        with pytest.raises(InputValidationError):
            success_stage.submit_message(msg)

    def test_invalid_input_wrong_type_raises(self, success_stage):
        msg = Message(topic="test", payload={"value": "not_an_int"})

        with pytest.raises(InputValidationError):
            success_stage.submit_message(msg)


class TestStageHandlerExecution:
    def test_handler_executed_successfully(self, create_stage):
        results = []

        class CaptureHandler(BaseHandler[InputModel, OutputModel]):
            def run(self, inputs: InputModel) -> OutputModel:
                result = OutputModel(result=f"captured_{inputs.value}")
                results.append(result)
                return result

        stage = create_stage(CaptureHandler(), name="CaptureStage")

        stage.submit({"value": 42})
        stage.task_queue.wait_all()

        assert len(results) == 1
        assert results[0].result == "captured_42"


class TestStagePolicy:
    def test_policy_resolve_inputs_called(self, create_stage):
        captured_inputs = []

        class CapturePolicy(DefaultStagePolicy[InputModel, OutputModel, Any, Any, Any]):
            def resolve_inputs(self, message, repo=None, cache=None):
                data = message.payload
                captured_inputs.append(data)
                return data

        stage = create_stage(
            SuccessHandler(), name="PolicyStage", policy=CapturePolicy()
        )

        stage.submit({"value": 99})
        stage.task_queue.wait_all()

        assert len(captured_inputs) == 1
        assert captured_inputs[0] == {"value": 99}

    def test_policy_on_success_called(self, create_stage):
        success_results = []

        class TrackingPolicy(
            DefaultStagePolicy[InputModel, OutputModel, Any, Any, Any]
        ):
            def on_success(
                self, task_id, message, result, broker=None, repo=None, cache=None
            ):
                success_results.append((task_id, result))

        stage = create_stage(
            SuccessHandler(), name="SuccessStage", policy=TrackingPolicy()
        )

        stage.submit({"value": 10})
        stage.task_queue.wait_all()
        stage.close()

        assert len(success_results) == 1
        assert success_results[0][1].result == "processed_10"

    def test_policy_on_error_called_on_handler_failure(self, create_stage):
        errors = []

        class TrackingPolicy(
            DefaultStagePolicy[InputModel, OutputModel, Any, Any, Any]
        ):
            def on_error(self, message, error, broker=None):
                errors.append(error)

        stage = create_stage(
            FailingHandler(), name="ErrorStage", policy=TrackingPolicy()
        )

        stage.submit({"value": 1})
        stage.close()

        assert len(errors) == 1
        assert "Handler failure!" in str(errors[0])

    def test_policy_intercept_skips_execution(self, create_stage):
        executed = []

        class InterceptingPolicy(
            DefaultStagePolicy[InputModel, OutputModel, Any, Any, Any]
        ):
            def intercept(self, message, broker=None, repo=None, cache=None):
                return InterceptionContext(
                    intercepted=True, payload=OutputModel(result="intercepted")
                )

        class TrackingHandler(BaseHandler[InputModel, OutputModel]):
            def run(self, inputs: InputModel) -> OutputModel:
                executed.append(True)
                return OutputModel(result="should_not_see")

        stage = create_stage(
            TrackingHandler(), name="InterceptStage", policy=InterceptingPolicy()
        )

        stage.submit({"value": 1})
        stage.task_queue.wait_all()

        assert len(executed) == 0


class TestStagePolicyBuilder:
    def test_builder_with_custom_hooks(self, create_stage):
        calls = []

        def on_success_cb(task_id, message, result, broker=None, repo=None, cache=None):  # noqa: ARG001
            calls.append(("success", result.result))

        policy = (
            StagePolicyBuilder[InputModel, OutputModel, Any, Any, Any]()
            .with_resolve_inputs(lambda msg, repo=None, cache=None: {"value": 777})
            .with_on_success(on_success_cb)
            .with_on_error(
                lambda msg, err, broker=None: calls.append(("error", str(err)))
            )
        )

        stage = create_stage(SuccessHandler(), name="BuilderStage", policy=policy)
        stage.submit({"value": 1})
        stage.close()

        assert len(calls) == 1
        assert calls[0] == ("success", "processed_777")

    def test_builder_on_error_catches_handler_failure(self, create_stage):
        errors = []

        policy = StagePolicyBuilder[
            InputModel, OutputModel, Any, Any, Any
        ]().with_on_error(lambda msg, err, broker=None: errors.append(str(err)))

        stage = create_stage(FailingHandler(), name="BuilderErrorStage", policy=policy)

        stage.submit({"value": 1})
        stage.close()

        assert len(errors) == 1
        assert "Handler failure!" in errors[0]


class TestStageLifecycle:
    def test_close_calls_handler_shutdown(self):
        handler = WarmupHandler()
        stage = Stage(handler=handler, name="LifecycleStage")

        assert handler.shutdown_called is False
        stage.close()
        assert handler.shutdown_called is True

    def test_close_closes_task_queue(self, success_stage):
        success_stage.close()

        with pytest.raises(RuntimeError, match="is closed"):
            success_stage.submit({"value": 1})


class TestStageName:
    def test_default_name_uses_handler_class(self, create_stage):
        stage = create_stage(SuccessHandler())

        assert "SuccessHandler" in stage.name

    def test_custom_name(self, create_stage):
        stage = create_stage(SuccessHandler(), name="CustomStageName")

        assert stage.name == "CustomStageName"


class TestStageInputTypeDetection:
    def test_detects_input_type_from_generic(self, create_stage):
        class TypedHandler(BaseHandler[InputModel, OutputModel]):
            def run(self, inputs: InputModel) -> OutputModel:
                return OutputModel(result="typed")

        stage = create_stage(TypedHandler())

        assert stage._input_model_type is InputModel

    def test_detects_input_type_from_run_annotation(self, create_stage):
        class AnnotatedHandler(BaseHandler[InputModel, OutputModel]):
            def run(self, inputs: InputModel) -> OutputModel:
                return OutputModel(result="annotated")

        stage = create_stage(AnnotatedHandler())

        assert stage._input_model_type is InputModel
