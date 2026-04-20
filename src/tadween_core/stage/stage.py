import functools
import queue
import threading
from concurrent.futures import Future
from logging import Logger, getLogger
from typing import Any, Generic, get_args, get_origin, get_type_hints

from pydantic import BaseModel, ValidationError

from tadween_core.broker import BaseMessageBroker, Message
from tadween_core.cache.base import BaseCache
from tadween_core.coord import (
    ResourceManager,
    StageContextConfig,
    WorkflowContext,
)
from tadween_core.exceptions import (
    HandlerError,
    InputValidationError,
    PolicyError,
    ResourceError,
    StageError,
)
from tadween_core.handler.base import BaseHandler
from tadween_core.repo.base import BaseArtifactRepo
from tadween_core.task_queue import BaseTaskQueue, init_queue
from tadween_core.task_queue.base import TaskEnvelope

from .policy import (
    ArtifactT,
    BucketSchemaT,
    DefaultStagePolicy,
    InputT,
    OutputT,
    PartNameT,
    StagePolicy,
)


class Stage(Generic[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]):
    """
    A stage binds a handler, policy, and task queue around a specific piece of work.

    Internally, a stage uses a bounded **stage queue** and a **collector thread**
    to decouple message submission from execution and provide back-pressure:

        submit_message → [stage queue] → collector thread → _process_message → task queue → callback

    * **Stage queue**: bounded internal buffer. When full, ``submit_message``
        blocks until a slot frees up.  ``queue_size=0`` (default) = unbounded.
    * **Collector thread**: daemon that drains the stage queue and feeds each
        message through ``_process_message`` (the policy lifecycle).
    * **Task registry**: maps ``message.id`` → ``task_id`` so callers can
        correlate a submission ID with the underlying worker via
        ``get_worker_task_id``.
    """

    def __init__(
        self,
        handler: BaseHandler[InputT, OutputT],
        *,
        name: str | None = None,
        policy: StagePolicy[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]
        | None = None,
        repo: BaseArtifactRepo | None = None,
        cache: BaseCache[BucketSchemaT] | None = None,
        logger: Logger | None = None,
        broker: BaseMessageBroker | None = None,
        task_queue: BaseTaskQueue[OutputT] | None = None,
        log_exc_info: bool = True,
        queue_size: int = 0,
        resource_manager: ResourceManager | None = None,
        demands: dict[str, float] | None = None,
        context_config: StageContextConfig | None = None,
    ):
        self.handler = handler
        self.name = name or f"Stage:{self.handler.__class__.__name__}"
        self.logger = logger or getLogger(f"tadween.stage.{self.name}")
        self.log_exc_info = log_exc_info

        self.policy = policy or DefaultStagePolicy()
        self.broker = broker
        self.repo = repo
        self.cache = cache
        self.resource_manager = resource_manager
        self.demands = demands

        self.context_config = context_config or StageContextConfig()
        if not self.context_config.context:
            self.context_config.context = WorkflowContext()

        # Introspect input type once at initialization
        self._input_model_type: type[InputT] | None = self._detect_input_type(handler)

        self.task_queue = task_queue or init_queue(
            executor="thread",
            name=f"TaskQueue-{self.name}",
        )

        # Internal queueing and collector
        self._stage_queue: queue.Queue[Message] = queue.Queue(maxsize=queue_size)
        self._task_registry: dict[str, str | None] = {}  # message.id -> task_id
        self._registry_lock = threading.Lock()
        self._stop_event = threading.Event()

        self._collector_thread = threading.Thread(
            target=self._collector_loop, name=f"Collector-{self.name}", daemon=True
        )
        self._collector_thread.start()

    def submit_message(self, message: Message, timeout: int | None = None) -> str:
        """
        Submit a message to the stage queue.

        Returns the message ID (for submission tracking). The worker task ID
        can later be resolved via ``get_worker_task_id``.

        Blocks if the stage queue is full, until a slot frees up or *timeout*
        expires. Raises ``RuntimeError`` if the stage is closed.
        """
        if self._stop_event.is_set():
            raise RuntimeError(f"Stage {self.name} is closed")

        with self._registry_lock:
            self._task_registry[message.id] = None

        # Block until a slot is free
        self._stage_queue.put(message, block=True, timeout=timeout)

        return message.id

    def _process_message(self, message: Message) -> bool:
        """
        Flow:
        1. Policy decides whether to PROCESS, SKIP, or CANCEL (on_submitted).
        2. Policy resolves raw inputs (Cache -> Payload -> Repo).
        3. Stage validates/converts raw inputs to Handler Input Model.
        4. TaskQueue executes Handler.
        5. Callback triggers Policy success/failure hooks.

        Returns:
            bool: True if task was enqueued, False otherwise.
        """

        # step 1: intercept
        try:
            context = self.policy.intercept(message, self.broker, self.repo, self.cache)
        except Exception as e:
            err = PolicyError(
                message=str(e),
                stage_name=self.name,
                policy_name=self.policy.__class__.__name__,
                method="intercept",
            )
            self.logger.error(
                f"Policy failed to intercept: {err}", exc_info=self.log_exc_info
            )
            self.policy.on_error(message, err, self.broker)
            return False
        if context and context.intercepted:
            self.logger.info(f"Intercepted. Reason: {context.reason}")
            return False

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
            self.logger.error(f"Policy failed to resolve inputs: {err}", exc_info=True)
            self.policy.on_error(message, err, self.broker)
            return False

        # step 3: validate
        try:
            input_data: InputT = self._enforce_input_type(raw_input)
        except Exception as e:
            err = (
                e
                if isinstance(e, StageError)
                else InputValidationError(str(e), stage_name=self.name)
            )
            self.policy.on_error(message, err, self.broker)
            return False

        # step 4: enqueue
        try:
            callback = functools.partial(self._on_task_done, message)
            # FIXME: `on_running` is currently disabled. Reason: not stable with process task queue.
            # on_running = functools.partial(self.policy.on_running, message)
            task_id = self.task_queue.submit(
                fn=self.handler.run,
                on_done=callback,
                inputs=input_data,
            )
            with self._registry_lock:
                self._task_registry[message.id] = task_id
            return True
        except Exception as e:
            err = (
                e
                if isinstance(e, StageError)
                else StageError(
                    message=f"Task submission failed: {e}",
                    stage_name=self.name,
                )
            )
            self.logger.error(str(err), exc_info=self.log_exc_info)
            self.policy.on_error(message, err, self.broker)
            return False

    def submit(
        self,
        input_data: InputT | dict,
        *,
        metadata: dict | None = None,
        topic: str = "N/A",
    ) -> str:
        """
        Convenience wrapper: submit raw input directly without constructing a Message.

        Wraps *input_data* in a ``Message`` and delegates to ``submit_message``.
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
            self.logger.critical(str(err), exc_info=self.log_exc_info)
            self.policy.on_error(message, err, broker=self.broker)
            return
        finally:
            if self.resource_manager and self.demands:
                self.resource_manager.release(self.demands)
            if self.context_config.context:
                if self.context_config.done_state_update:
                    self.context_config.context.apply_state(
                        self.context_config.done_state_update,
                        metadata=message.metadata,
                    )
                if self.context_config.notify_events:
                    self.context_config.context.notify(
                        self.context_config.notify_events,
                        n=self.context_config.n_notify,
                    )

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
            self.logger.error(str(err), exc_info=self.log_exc_info)
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
                self.logger.error(
                    f"Policy.on_success failed: {err}", exc_info=self.log_exc_info
                )

                self.policy.on_error(message, err, broker=self.broker)
        else:
            # Preserve the original exception as the cause
            err = HandlerError(
                message=str(envelope.error),
                stage_name=self.name,
                task_id=task_id,
            )
            if envelope.error:
                err.__cause__ = envelope.error

            self.logger.error(
                f"Task failed: {err}",
                exc_info=envelope.error if self.log_exc_info else False,
            )
            if self.log_exc_info and envelope.traceback:
                self.logger.error(
                    f"Worker traceback for task {task_id}:\n{envelope.traceback}",
                )

            self.policy.on_error(message, err, broker=self.broker)

    def wait_all(self, timeout: float | None = None):
        """
        Wait for all messages in the stage queue to be processed and
        all tasks in the task queue to complete.
        """
        self._stage_queue.join()
        self.task_queue.wait_all(timeout=timeout)

    def close(self, timeout=5, force: bool = False):
        """
        Tear down the stage.

        Unless *force*, drains the stage queue and waits for all tasks to
        finish (up to *timeout* seconds). Then signals the collector thread
        to stop, joins it, shuts down the handler, and closes the task queue.

        Args:
            timeout: Seconds to wait for pending work. Defaults to 5.
            force: If True, skip the graceful drain and shut down immediately.
        """
        if self._stop_event.is_set():
            return

        if not force:
            try:
                self.wait_all(timeout=timeout)
            except Exception as e:
                self.logger.warning(f"Graceful wait_all failed during close: {e}")

        self._stop_event.set()
        # Wake up collector if it's waiting
        self._stage_queue.put(None)
        if self._collector_thread.is_alive():
            self._collector_thread.join(timeout=2.0)
        self.handler.shutdown()
        self.task_queue.close(force=force)

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

    def get_worker_task_id(self, message_id: str) -> str | None:
        """
        Resolve the internal task queue ID from the message ID.
        """
        with self._registry_lock:
            return self._task_registry.get(message_id)

    def _collector_loop(self):
        """
        Daemon loop that drains the stage queue and processes each message.

        Coordination lifecycle (per message):

        1. **Logical backpressure**: block until the deferral predicate clears.
        2. **Physical backpressure**: acquire physical resources via ResourceManager.
        3. **Process**: run the policy lifecycle (intercept -> resolve -> validate -> enqueue).
        4. **Release on done**: Release both physical acquisition, and the logical claim.
        5. **Rollback on failure**: if enqueuing fails, roll back both the logical claim and the physical acquisition.
        """
        while not self._stop_event.is_set():
            try:
                message = self._stage_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                if message is None:
                    break

                # Logical Backpressure (Deferral)
                logical_acquired = False
                if self.context_config.defer_predicate and self.context_config.context:
                    try:
                        self.context_config.context.wait_for(
                            event_name=self.context_config.defer_event,
                            predicate=self.context_config.defer_predicate,
                            metadata=message.metadata,
                            poll_interval=self.context_config.defer_poll_interval,
                            timeout=self.context_config.defer_timeout,
                            update_on_acquire=self.context_config.defer_state_update,
                        )
                        logical_acquired = True
                    except TimeoutError as e:
                        self.logger.warning(
                            f"Message {message.id} deferred too long: {e}"
                        )
                        self.policy.on_error(
                            message, StageError(str(e), self.name), self.broker
                        )
                        continue

                enqueued = False
                physical_acquired = False
                try:
                    # Physical Backpressure (Resource Management)
                    if self.resource_manager and self.demands:
                        try:
                            self.resource_manager.acquire(self.demands)
                            physical_acquired = True
                        except ResourceError:
                            self.logger.warning(
                                f"Resource acquisition aborted (manager shutdown) for message {message.id}"
                            )
                            # Implicitly triggers rollback of Step 1 in 'finally' block
                            continue

                    # Process and Enqueue
                    enqueued = self._process_message(message)
                finally:
                    if not enqueued:
                        # Release physical resources if they were acquired but enqueuing failed
                        if physical_acquired:
                            self.resource_manager.release(self.demands)

                        # Rollback logical state changes if any were applied during wait_for.
                        if logical_acquired and self.context_config.context:
                            if self.context_config.rollback_state_update:
                                self.context_config.context.apply_state(
                                    self.context_config.rollback_state_update,
                                    metadata=message.metadata,
                                )
                            elif isinstance(
                                self.context_config.defer_state_update, dict
                            ):
                                rollback = {
                                    k: -v
                                    for k, v in self.context_config.defer_state_update.items()
                                }
                                self.context_config.context.apply_state(rollback)
                            else:
                                self.logger.warning(
                                    f"Stage {self.name}: Cannot automatically rollback callable defer_state_update. "
                                    "Please provide rollback_state_update."
                                )
            except Exception as e:
                self.logger.error(
                    f"Unexpected error during processing message. Error {e}",
                    exc_info=True,
                )
            finally:
                self._stage_queue.task_done()

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
                self.logger.error(str(err), exc_info=self.log_exc_info)
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
