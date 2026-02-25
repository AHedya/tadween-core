# tadween_core/workflow/stage.py
import functools
from concurrent.futures import Future
from logging import Logger, getLogger
from typing import Annotated, Any, Generic, get_args, get_origin, get_type_hints

from pydantic import BaseModel, ValidationError
from typing_extensions import TypeVar

from tadween_core.broker import BaseMessageBroker, Message
from tadween_core.cache import Cache
from tadween_core.exceptions import (
    HandlerError,
    InputValidationError,
    PolicyError,
    StageError,
)
from tadween_core.handler.base import BaseHandler, InputT, OutputT
from tadween_core.repo.base import BaseArtifactRepo
from tadween_core.task_queue import BaseTaskQueue, init_queue
from tadween_core.task_queue.base import TaskEnvelope

from .policy import DefaultStagePolicy, ProcessingAction, StagePolicy

BucketSchemaT = TypeVar("BucketSchemaT", default=Any)


class Stage(Generic[InputT, OutputT, BucketSchemaT]):
    def __init__(
        self,
        handler: BaseHandler[InputT, OutputT],
        *,
        name: str | None = None,
        policy: StagePolicy[InputT, OutputT, Any] | None = None,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
        logger: Logger | None = None,
        broker: BaseMessageBroker | None = None,
        task_queue: BaseTaskQueue[OutputT] | None = None,
    ):
        self.handler = handler
        self.name = name or f"Stage:{self.handler.__class__.__name__}"
        self.logger = logger or getLogger(self.name)

        self.policy = policy or DefaultStagePolicy()
        self.broker = broker
        self.repo = repo
        self.cache = cache

        self._active_jobs: Annotated[
            dict[str, str], "Lookup message_id -> task_id (internal per stage)"
        ] = {}

        # Introspect input type once at initialization
        self._input_model_type: type[InputT] | None = self._detect_input_type(handler)

        if task_queue:
            self.task_queue = task_queue
        else:
            self.task_queue = init_queue(
                executor="thread",
                name=f"TaskQueue-{self.name}",
            )

    def submit_message(self, message: Message) -> str:
        """
        Submit a message to be processed.

        Flow:
        1. Policy decides whether to process (on_submitted returns ProcessingAction).
        2. Policy resolves raw inputs (Cache -> Payload -> Repo).
        3. Stage validates/converts raw inputs to Handler Input Model.
        4. TaskQueue executes Handler.
        5. Callback triggers Policy success/failure hooks.
        """

        # Step 1: Check if we should process, skip, or cancel
        decision = self.policy.on_submitted(message, repo=self.repo, cache=self.cache)

        if decision == ProcessingAction.CANCEL:
            # Fire on_error and return without processing
            err = RuntimeError(
                f"Stage '{self.name}' cancelled by policy for message {message.id}"
            )
            self.logger.warning(f"Stage cancelled: {err}")
            self.policy.on_error(message, err, broker=self.broker)
            return message.id

        if decision == ProcessingAction.SKIP:
            # Skip handler execution, treat as completed success
            self.logger.info(f"Stage '{self.name}' skipping message {message.id}")
            self.policy.on_success(
                task_id="",
                message=message,
                result=None,
                broker=self.broker,
                repo=self.repo,
                cache=self.cache,
            )
            return message.id

        # Step 2: Normal flow - resolve inputs
        # decision == ProcessingAction.PROCESS
        try:
            raw_input = self.policy.resolve_inputs(
                message, repo=self.repo, cache=self.cache
            )
        except Exception as e:
            err = PolicyError(
                message=str(e),
                stage_name=self.name,
                policy_name=self.policy.__class__.__name__,
                method="resolve_inputs",
            )
            self.logger.error(f"Policy failed to resolve inputs: {err}")
            self.policy.on_error(message, err)
            raise err from e

        try:
            input_data: InputT = self._enforce_input_type(raw_input)
        except Exception as e:
            # If it's already a StageError (from _enforce_input_type), just re-raise and notify
            err = (
                e
                if isinstance(e, StageError)
                else InputValidationError(str(e), stage_name=self.name)
            )
            self.policy.on_error(message, err)
            raise err from e

        callback = functools.partial(self._on_task_done, message)

        # on_running, on_submit will be overridden here to do both. However, on_running isn't set here; to be implemented if needed
        # stage logic, and task-level lifecycle
        task_id = self.task_queue.submit(
            fn=self.handler.run, on_done=callback, inputs=input_data
        )

        self._active_jobs[message.id] = task_id
        return task_id

    def _enforce_input_type(self, raw_input: Any) -> InputT:
        """
        Ensures the data matches InputT.
        """
        if self._input_model_type is None:
            return raw_input

        if isinstance(raw_input, self._input_model_type):
            return raw_input

        if isinstance(raw_input, dict):
            try:
                return self._input_model_type(**raw_input)
            except ValidationError as e:
                err = InputValidationError(
                    message=f"Pydantic validation failed: {e}",
                    stage_name=self.name,
                )
                self.logger.error(str(err))
                raise err from e
            except Exception as e:
                err = InputValidationError(
                    message=f"Unexpected error at validation step: {e}",
                    stage_name=self.name,
                )
                raise err from e

        err = InputValidationError(
            message=f"Expected dict or {self._input_model_type.__name__}, got {type(raw_input).__name__}",
            stage_name=self.name,
        )
        raise err

    def _on_task_done(
        self, message: Message, task_id: str, future: Future[TaskEnvelope[OutputT]]
    ):
        """
        callback runs after submitted task finishes execution in task queue.
        Args:
            message (Message): Injected by the stage.
            task_id (str): Injected by the task queue
            future (Future[TaskEnvelope[OutputT]]): Injected by the callback once task is done.
        """
        try:
            envelope = future.result()
            message.metadata.update({"current_stage": self.name})

            self.policy.on_done(message=message, envelope=envelope)
            if envelope.success:
                try:
                    self.policy.on_success(
                        task_id=task_id,
                        message=message,
                        result=envelope.payload,
                        broker=self.broker,
                        repo=self.repo,
                        cache=self.cache,
                    )
                except Exception as e:
                    err = PolicyError(
                        message=str(e),
                        stage_name=self.name,
                        policy_name=self.policy.__class__.__name__,
                        method="on_success",
                        task_id=task_id,
                    )
                    self.logger.error(f"Policy.on_success failed: {err}")
                    self.policy.on_error(message, err)
            else:
                err = HandlerError(
                    message=str(envelope.error),
                    stage_name=self.name,
                    task_id=task_id,
                )
                self.logger.error(f"Task failed: {err}")
                self.policy.on_error(message, err)
        except Exception as e:
            err = StageError(
                message=f"Critical error in stage callback: {e}",
                stage_name=self.name,
                task_id=task_id,
            )
            self.logger.critical(str(err))
            self.policy.on_error(
                message,
                err,
                broker=self.broker,
            )

    def _detect_input_type(self, handler: BaseHandler) -> type[InputT] | None:
        """
        Detect Input Type with a fallback strategy:
        1. Check explicit type hint on `run(inputs: ...)`
        2. Check Generic Class definition `class MyHandler(BaseHandler[InputT, OutputT])`
        """
        try:
            method_hints = get_type_hints(handler.run)
            if "inputs" in method_hints:
                return method_hints["inputs"]
        except Exception:
            pass

        # Strategy 2: Inspect Class Generics (The "Inferred" way)
        current_class = handler.__class__

        while current_class is not object:
            # __orig_bases__ contains the raw Generic types (e.g. BaseHandler[In, Out])
            orig_bases = getattr(current_class, "__orig_bases__", [])
            for base in orig_bases:
                origin = get_origin(base)
                # Check if this base class is actually BaseHandler (or a subclass of it)
                if origin and issubclass(origin, BaseHandler):
                    args = get_args(base)
                    # BaseHandler[InputT, OutputT] -> args[0] is InputT
                    if args and len(args) >= 1:
                        input_type = args[0]
                        # Sanity check: Ensure it's not a TypeVar (which happens if the user
                        # made an intermediate generic class without defining the types yet)
                        if isinstance(input_type, type) and issubclass(
                            input_type, BaseModel
                        ):
                            return input_type

            current_class = current_class.__base__

        self.logger.warning(
            f"Could not detect input type for {handler.__class__.__name__}. "
            "Please explicitly type hint 'run(inputs: MyModel)' or inherit 'BaseHandler[MyModel, ...]'"
        )
        return None

    def close(self):
        """Cleanup stage resources"""
        self.task_queue.close()
        if hasattr(self.handler, "shutdown"):
            self.handler.shutdown()
