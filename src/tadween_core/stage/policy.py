from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated, Any, Generic, TypeAlias

from pydantic import BaseModel
from typing_extensions import TypeVar

from tadween_core.broker import BaseMessageBroker, Message
from tadween_core.cache.cache import Cache
from tadween_core.repo.base import BaseArtifactRepo
from tadween_core.task_queue.base import TaskEnvelope

BucketSchemaT = TypeVar("BucketSchemaT", default=Any)
InputT = TypeVar("InputT", default=Any, bound=BaseModel)
OutputT = TypeVar("OutputT", default=Any, bound=BaseModel)


class ProcessingAction(StrEnum):
    """Decision returned by on_submitted to control stage execution flow."""

    PROCESS = auto()
    """Normal execution: resolve inputs and run the handler."""

    SKIP = auto()
    """Skip handler execution: fire on_success with None result, treat as completed."""

    CANCEL = auto()
    """Cancel entirely: fire on_error, do not proceed with execution."""


class StagePolicy(ABC, Generic[InputT, OutputT, BucketSchemaT]):
    """
    Defines the lifecycle and behavioral rules of a Stage.

    Responsibilities:
    1. Input Resolution: Decides where data comes from (Cache -> Payload -> Repo).
    2. Lifecycle Hooks: Defines behavior at submission, execution, and completion.
    3. Output Persistence: Decides where results go (Save to Repo? Update Cache?).
    """

    @abstractmethod
    def resolve_inputs(
        self,
        message: Message,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InputT | dict:
        """Determine the input data for the Handler."""
        pass

    @abstractmethod
    def on_submitted(
        self,
        message: Message,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> ProcessingAction:
        """
        Called immediately after the task is submitted to the queue (Main Process).

        Return ProcessingAction:
        - PROCESS: Normal execution (default)
        - SKIP: Skip handler, fire on_success with None result
        - CANCEL: Cancel entirely, fire on_error
        """
        return ProcessingAction.PROCESS

    @abstractmethod
    def on_running(self, task_id: str, message: Message):
        """Called when the task starts executing (Worker Process/Thread)."""
        pass

    @abstractmethod
    def on_done(self, message: Message, envelope: TaskEnvelope[OutputT]):
        """Called once the task finishes. Doesn't matter what the status is."""
        pass

    @abstractmethod
    def on_success(
        self,
        task_id: str,
        message: Message,
        result: OutputT,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ):
        """Called when the Handler completes successfully."""
        pass

    @abstractmethod
    def on_error(
        self,
        message: Message,
        error: Exception,
        broker: BaseMessageBroker | None = None,
    ):
        """Called when the Handler fails."""
        pass


class DefaultStagePolicy(StagePolicy[InputT, OutputT, BucketSchemaT]):
    """
    Default implementation of StagePolicy with sensible defaults.

    - resolve_inputs: Returns message.payload if available, otherwise empty dict
    - All lifecycle hooks: No-op (pass)
    """

    def resolve_inputs(
        self,
        message: Message,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InputT | dict:
        """Return message payload or empty dict if not available."""
        return getattr(message, "payload", {})

    def on_submitted(
        self,
        message: Message,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> ProcessingAction:
        """Default: always process the work."""
        return ProcessingAction.PROCESS

    def on_running(self, task_id: str, message: Message):
        """No-op: Default behavior when task starts running."""
        pass

    def on_done(self, message: Message, envelope: TaskEnvelope[OutputT]):
        pass

    def on_success(
        self,
        task_id: str,
        message: Message,
        result: OutputT,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ):
        """No-op: Default behavior on successful completion."""
        pass

    def on_error(
        self,
        message: Message,
        error: Exception,
        broker: BaseMessageBroker | None = None,
    ):
        """No-op: Default behavior on error."""
        pass


## builder and factory function

ResolveInputsFn: TypeAlias = Callable[
    [Message, BaseArtifactRepo | None, Cache[BucketSchemaT] | None], InputT | dict
]

OnSubmittedFn: TypeAlias = Callable[
    [Message, BaseArtifactRepo | None, Cache[BucketSchemaT] | None], ProcessingAction
]

OnRunningFn: TypeAlias = Callable[[Annotated[str, "task_id"], Message], None]

OnDoneFn: TypeAlias = Callable[[Message, TaskEnvelope[OutputT]], None]

OnSuccessFn: TypeAlias = Callable[
    [
        Annotated[str, "task_id"],
        Message,
        OutputT,
        BaseMessageBroker | None,
        BaseArtifactRepo | None,
        Cache[BucketSchemaT] | None,
    ],
    None,
]

OnErrorFn: TypeAlias = Callable[[Message, Exception, BaseMessageBroker | None], None]


class StagePolicyBuilder(
    DefaultStagePolicy[InputT, OutputT, BucketSchemaT],
    Generic[InputT, OutputT, BucketSchemaT],
):
    """
    Builder for creating StagePolicy instances with custom lifecycle hooks.

    Generic types are propagated to callback signatures for proper type checking.

    Example:
        policy = (StagePolicyBuilder[MyInput, MyOutput, dict]()
            .with_on_error(lambda msg, err, broker: print(f"Error: {err}"))
            .with_on_success(lambda tid, msg, result, **_: save_result(result))
        )
        # 'result' parameter will have type MyOutput
    """

    def __init__(self):
        super().__init__()
        self._resolve_inputs_fn: ResolveInputsFn | None = None
        self._on_submitted_fn: OnSubmittedFn | None = None
        self._on_running_fn: OnRunningFn | None = None
        self._on_done_fn: OnDoneFn | None = None
        self._on_success_fn: OnSuccessFn | None = None
        self._on_error_fn: OnErrorFn | None = None

    def with_resolve_inputs(
        self,
        fn: Callable[
            [Message, BaseArtifactRepo | None, Cache[BucketSchemaT] | None],
            InputT | dict,
        ],
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT]":
        """Override input resolution logic. Returns InputT or dict."""
        self._resolve_inputs_fn = fn
        return self

    def with_on_submitted(
        self,
        fn: Callable[
            [Message, BaseArtifactRepo | None, Cache[BucketSchemaT] | None],
            ProcessingAction,
        ],
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT]":
        """Override on_submitted hook to return ProcessingAction."""
        self._on_submitted_fn = fn
        return self

    def with_on_running(
        self, fn: Callable[[str, Message], None]
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT]":
        """Override on_running hook."""
        self._on_running_fn = fn
        return self

    def with_on_done(
        self, fn: Callable[[Message, TaskEnvelope[OutputT]], None]
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT]":
        """Override on_done hook. Envelope contains OutputT."""
        self._on_done_fn = fn
        return self

    def with_on_success(
        self,
        fn: Callable[
            [
                str,  # task_id
                Message,
                OutputT,  # result - properly typed with OutputT generic!
                BaseMessageBroker | None,
                BaseArtifactRepo | None,
                Cache[BucketSchemaT] | None,  # cache properly typed with BucketSchemaT!
            ],
            None,
        ],
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT]":
        """
        Override on_success hook.

        Args:
            fn: Callback where 'result' parameter will be typed as OutputT
        """
        self._on_success_fn = fn
        return self

    def with_on_error(
        self,
        fn: Callable[[Message, Exception, BaseMessageBroker | None], None],
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT]":
        """Override on_error hook."""
        self._on_error_fn = fn
        return self

    # Implement StagePolicy interface using configured callbacks

    def resolve_inputs(
        self,
        message: Message,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InputT | dict:
        if self._resolve_inputs_fn:
            return self._resolve_inputs_fn(message, repo, cache)
        return super().resolve_inputs(message, repo, cache)

    def on_submitted(
        self,
        message: Message,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> ProcessingAction:
        if self._on_submitted_fn:
            return self._on_submitted_fn(message, repo, cache)
        return super().on_submitted(message, repo, cache)

    def on_running(self, task_id: str, message: Message):
        if self._on_running_fn:
            self._on_running_fn(task_id, message)
        else:
            super().on_running(task_id, message)

    def on_done(self, message: Message, envelope: TaskEnvelope[OutputT]):
        if self._on_done_fn:
            self._on_done_fn(message, envelope)
        else:
            super().on_done(message, envelope)

    def on_success(
        self,
        task_id: str,
        message: Message,
        result: OutputT,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ):
        if self._on_success_fn:
            self._on_success_fn(task_id, message, result, broker, repo, cache)
        else:
            super().on_success(task_id, message, result, broker, repo, cache)

    def on_error(
        self,
        message: Message,
        error: Exception,
        broker: BaseMessageBroker | None = None,
    ):
        if self._on_error_fn:
            self._on_error_fn(message, error, broker)
        else:
            super().on_error(message, error, broker)


# Type-aware factory function
def create_stage_policy() -> StagePolicyBuilder[Any, Any, Any]:
    """
    Create a new stage policy builder with generic types.

    For proper type checking, specify the generics explicitly:

    Example:
        # Typed version
        policy = StagePolicyBuilder[MyInput, MyOutput, dict]().with_on_error(...)

        # Or use create helper with explicit types
        policy: StagePolicyBuilder[MyInput, MyOutput, dict] = create_stage_policy()
        policy = policy.with_on_success(lambda tid, msg, result, **_: ...)
        # 'result' will be typed as MyOutput
    """
    return StagePolicyBuilder()
