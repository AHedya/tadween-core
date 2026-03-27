import time
from typing import Any

import pytest
from pydantic import BaseModel

from tadween_core.broker import Message
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


class TestStageSubmission:
    def test_submit_message_returns_task_id(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="TestStage")

        msg = Message(topic="test", payload={"value": 42})
        task_id = stage.submit_message(msg)

        assert task_id is not None
        assert isinstance(task_id, str)
        stage.task_queue.wait_all()
        stage.close()

    def test_submit_convenience_method(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="TestStage")

        input_data = InputModel(value=100)
        task_id = stage.submit(input_data)

        assert task_id is not None
        stage.task_queue.wait_all()
        stage.close()

    def test_submit_with_dict_payload(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="TestStage")

        task_id = stage.submit({"value": 50})

        assert task_id is not None
        stage.task_queue.wait_all()
        stage.close()

    def test_submit_with_metadata(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="TestStage")

        task_id = stage.submit({"value": 1}, metadata={"trace_id": "abc123"})

        assert task_id is not None
        stage.task_queue.wait_all()
        stage.close()


class TestStageInputValidation:
    def test_valid_dict_input_converted(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="TestStage")

        msg = Message(topic="test", payload={"value": 10})
        task_id = stage.submit_message(msg)

        stage.task_queue.wait_all()
        assert task_id is not None
        stage.close()

    def test_valid_model_input_passed_through(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="TestStage")

        msg = Message(topic="test", payload=InputModel(value=20))
        task_id = stage.submit_message(msg)

        stage.task_queue.wait_all()
        assert task_id is not None
        stage.close()

    def test_invalid_input_missing_field_raises(self):
        from tadween_core.exceptions import InputValidationError

        handler = SuccessHandler()
        stage = Stage(handler=handler, name="ValidationStage")

        msg = Message(topic="test", payload={"wrong_key": 1})

        with pytest.raises(InputValidationError):
            stage.submit_message(msg)

        stage.close()

    def test_invalid_input_wrong_type_raises(self):
        from tadween_core.exceptions import InputValidationError

        handler = SuccessHandler()
        stage = Stage(handler=handler, name="TypeErrorStage")

        msg = Message(topic="test", payload={"value": "not_an_int"})

        with pytest.raises(InputValidationError):
            stage.submit_message(msg)

        stage.close()


class TestStageHandlerExecution:
    def test_handler_executed_successfully(self):
        results = []

        class CaptureHandler(BaseHandler[InputModel, OutputModel]):
            def run(self, inputs: InputModel) -> OutputModel:
                result = OutputModel(result=f"captured_{inputs.value}")
                results.append(result)
                return result

        handler = CaptureHandler()
        stage = Stage(handler=handler, name="CaptureStage")

        stage.submit({"value": 42})
        stage.task_queue.wait_all()

        assert len(results) == 1
        assert results[0].result == "captured_42"
        stage.close()


class TestStagePolicy:
    def test_policy_resolve_inputs_called(self):
        captured_inputs = []

        class CapturePolicy(DefaultStagePolicy[InputModel, OutputModel, Any, Any, Any]):
            def resolve_inputs(self, message, repo=None, cache=None):
                data = message.payload
                captured_inputs.append(data)
                return data

        handler = SuccessHandler()
        policy = CapturePolicy()
        stage = Stage(handler=handler, name="PolicyStage", policy=policy)

        stage.submit({"value": 99})
        stage.task_queue.wait_all()

        assert len(captured_inputs) == 1
        assert captured_inputs[0] == {"value": 99}
        stage.close()

    def test_policy_on_success_called(self):
        success_results = []

        class TrackingPolicy(
            DefaultStagePolicy[InputModel, OutputModel, Any, Any, Any]
        ):
            def on_success(
                self, task_id, message, result, broker=None, repo=None, cache=None
            ):
                success_results.append((task_id, result))

        handler = SuccessHandler()
        policy = TrackingPolicy()
        stage = Stage(handler=handler, name="SuccessStage", policy=policy)

        stage.submit({"value": 10})
        stage.task_queue.wait_all()

        assert len(success_results) == 1
        assert success_results[0][1].result == "processed_10"
        stage.close()

    def test_policy_on_error_called_on_handler_failure(self):
        errors = []

        class TrackingPolicy(
            DefaultStagePolicy[InputModel, OutputModel, Any, Any, Any]
        ):
            def on_error(self, message, error, broker=None):
                errors.append(error)

        handler = FailingHandler()
        policy = TrackingPolicy()
        stage = Stage(handler=handler, name="ErrorStage", policy=policy)

        stage.submit({"value": 1})
        stage.task_queue.wait_all()

        assert len(errors) == 1
        assert "Handler failure!" in str(errors[0])
        stage.close()

    def test_policy_intercept_skips_execution(self):
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

        handler = TrackingHandler()
        policy = InterceptingPolicy()
        stage = Stage(handler=handler, name="InterceptStage", policy=policy)

        stage.submit({"value": 1})
        stage.task_queue.wait_all()

        assert len(executed) == 0
        stage.close()


class TestStagePolicyBuilder:
    def test_builder_with_custom_hooks(self):
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

        handler = SuccessHandler()
        stage = Stage(handler=handler, name="BuilderStage", policy=policy)

        stage.submit({"value": 1})
        stage.task_queue.wait_all()

        assert len(calls) == 1
        assert calls[0] == ("success", "processed_777")
        stage.close()

    def test_builder_on_error_catches_handler_failure(self):
        errors = []

        policy = StagePolicyBuilder[
            InputModel, OutputModel, Any, Any, Any
        ]().with_on_error(lambda msg, err, broker=None: errors.append(str(err)))

        handler = FailingHandler()
        stage = Stage(handler=handler, name="BuilderErrorStage", policy=policy)

        stage.submit({"value": 1})
        stage.task_queue.wait_all()

        assert len(errors) == 1
        assert "Handler failure!" in errors[0]
        stage.close()


class TestStageLifecycle:
    def test_close_calls_handler_shutdown(self):
        handler = WarmupHandler()
        stage = Stage(handler=handler, name="LifecycleStage")

        assert handler.shutdown_called is False
        stage.close()
        assert handler.shutdown_called is True

    def test_close_closes_task_queue(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="QueueStage")

        stage.close()

        with pytest.raises(RuntimeError, match="is closed"):
            stage.submit({"value": 1})


class TestStageName:
    def test_default_name_uses_handler_class(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler)

        assert "SuccessHandler" in stage.name

    def test_custom_name(self):
        handler = SuccessHandler()
        stage = Stage(handler=handler, name="CustomStageName")

        assert stage.name == "CustomStageName"


class TestStageInputTypeDetection:
    def test_detects_input_type_from_generic(self):
        class TypedHandler(BaseHandler[InputModel, OutputModel]):
            def run(self, inputs: InputModel) -> OutputModel:
                return OutputModel(result="typed")

        handler = TypedHandler()
        stage = Stage(handler=handler)

        assert stage._input_model_type is InputModel

    def test_detects_input_type_from_run_annotation(self):
        class AnnotatedHandler(BaseHandler[InputModel, OutputModel]):
            def run(self, inputs: InputModel) -> OutputModel:
                return OutputModel(result="annotated")

        handler = AnnotatedHandler()
        stage = Stage(handler=handler)

        assert stage._input_model_type is InputModel
