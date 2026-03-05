import functools
from concurrent.futures import Future
from logging import Logger, getLogger
from typing import Any, Generic, get_args, get_origin, get_type_hints

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

from .policy import DefaultStagePolicy, StagePolicy

BucketSchemaT = TypeVar("BucketSchemaT", default=Any)


class Stage(Generic[InputT, OutputT, BucketSchemaT]):
    def __init__(
        self,
        handler: BaseHandler[InputT, OutputT],
        *,
        name: str | None = None,
        policy: StagePolicy[InputT, OutputT, BucketSchemaT] | None = None,
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

        # Introspect input type once at initialization
        self._input_model_type: type[InputT] | None = self._detect_input_type(handler)

        self.task_queue = task_queue or init_queue(
            executor="thread",
            name=f"TaskQueue-{self.name}",
        )

    def submit_message(self, message: Message) -> str | None:
        """
        Submit a message to be processed.

        Flow:
        1. Policy decides whether to PROCESS, SKIP, or CANCEL (on_submitted).
        2. Policy resolves raw inputs (Cache -> Payload -> Repo).
        3. Stage validates/converts raw inputs to Handler Input Model.
        4. TaskQueue executes Handler.
        5. Callback triggers Policy success/failure hooks.

        Returns:
            task_id (str | None): returns task_id _if_ any tasks are enqueued. Returns None if task is skipped or cancelled
        """
        # TODO: Error handling is repeated

        # step 1: intercept
        try:
            intercepted = self.policy.intercept(
                message, repo=self.repo, cache=self.cache
            )
        except Exception as e:
            err = PolicyError(
                message=str(e),
                stage_name=self.name,
                policy_name=self.policy.__class__.__name__,
                method="intercept",
            )
            self.logger.error(f"Policy failed to intercept: {err}")
            self.policy.on_error(message, err, self.broker)
            raise err from e
        if intercepted:
            return

        # Step 2: resolve inputs
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
            self.policy.on_error(message, err, self.broker)
            raise err from e

        # step 3: validate
        try:
            input_data: InputT = self._enforce_input_type(raw_input)
        except Exception as e:
            # If it's already a StageError (from _enforce_input_type), re-raise and notify
            err = (
                e
                if isinstance(e, StageError)
                else InputValidationError(str(e), stage_name=self.name)
            )
            self.policy.on_error(message, err, self.broker)
            raise err from e

        # step4: enqueue
        callback = functools.partial(self._on_task_done, message)
        on_running = functools.partial(self.policy.on_running, message)
        task_id = self.task_queue.submit(
            fn=self.handler.run,
            on_done=callback,
            inputs=input_data,
            on_running=on_running,
        )

        return task_id

    def submit(
        self,
        input_data: InputT | dict,
        *,
        metadata: dict | None = None,
        topic: str = "N/A",
    ):
        """
        Submit raw input directly, without constructing a Message manually.
        """
        payload = (
            input_data if isinstance(input_data, dict) else input_data.model_dump()
        )
        message = Message(
            topic=topic,
            payload=payload,
            metadata=metadata,
        )
        return self.submit_message(message)

    def _on_task_done(
        self, message: Message, task_id: str, future: Future[TaskEnvelope[OutputT]]
    ):
        """
        callback runs after submitted task finishes execution in task queue.
        Args:
            message (Message): Injected by the stage.
            task_id (str): Injected by the task queue
            future : Injected by the callback once task is done.
        """
        try:
            envelope = future.result()
        except Exception as e:
            err = StageError(
                message=f"Critical error retrieving task result: {e}",
                stage_name=self.name,
                task_id=task_id,
            )
            self.logger.critical(str(err))
            self.policy.on_error(message, err, broker=self.broker)
            return

        message.metadata.update({"current_stage": self.name})

        try:
            self.policy.on_done(message=message, envelope=envelope)
        except Exception as e:
            err = PolicyError(
                message=f"on_done raised: {e}",
                stage_name=self.name,
                policy_name=self.policy.__class__.__name__,
                method="on_done",
                task_id=task_id,
            )
            self.logger.error(str(err))
            self.policy.on_error(message, err, broker=self.broker)
            return
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

                self.policy.on_error(message, err, broker=self.broker)
        else:
            err = HandlerError(
                message=str(envelope.error),
                stage_name=self.name,
                task_id=task_id,
            )
            self.logger.error(f"Task failed: {err}")
            self.policy.on_error(message, err, broker=self.broker)

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

        # Inspect Class Generics (The "Inferred" way)
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
        self.handler.shutdown()
        self.task_queue.close()

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
