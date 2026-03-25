from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any, Generic, TypeAlias

from pydantic import BaseModel
from typing_extensions import TypeVar

from tadween_core.broker import BaseMessageBroker, Message
from tadween_core.cache.cache import Cache
from tadween_core.repo.base import BaseArtifactRepo
from tadween_core.task_queue.base import TaskEnvelope
from tadween_core.types.artifact.base import BaseArtifact

ArtifactT = TypeVar("ArtifactT", default=Any, bound=BaseArtifact)
PartNameT = TypeVar("PartNameT", default=str, bound=str)
BucketSchemaT = TypeVar("BucketSchemaT", default=Any)
InputT = TypeVar("InputT", default=Any, bound=BaseModel)
OutputT = TypeVar("OutputT", default=Any, bound=BaseModel)

T = TypeVar("T", default=Any, bound=BaseModel)


@dataclass
class InterceptionContext(Generic[OutputT]):
    intercepted: bool
    payload: OutputT | None = None
    reason: str | None = None


class StagePolicy(ABC, Generic[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]):
    """
    Lifecycle contract for a Stage.

    A policy is composed of two kinds of methods:

    Control methods: return values the Stage acts on:
        - intercept(message, repo, cache) -> bool
        - resolve_inputs(message, repo, cache) -> InputT | dict

    Notification hooks: void, fired by the Stage at fixed points:
        - on_running, on_done, on_success, on_error

    Lifecycle order:
        intercept
        └── True:  stage stops. policy owns all further execution.
        └── False:
            resolve_inputs
            on_running
            on_done
            on_success | on_error

    Metadata contract:
        `intercept` may freely mutate `message.metadata` to annotate the reason
        for its decision (e.g. skip_reason, cache_hit).
    """

    @abstractmethod
    def resolve_inputs(
        self,
        message: Message,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InputT | dict:
        """
        Control hook: called after a False intercept, before task execution.
        Composes the input that will be passed to the handler.

        Returns:
            InputT instance, or a dict that the stage will convert to InputT.

        Raise an exception to abort execution. The stage will catch it,
        wrap it in InputValidationError, and fire on_error.
        """
        pass

    @abstractmethod
    def intercept(
        self,
        message: Message,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InterceptionContext[OutputT]:
        """
        Control hook:  called first on every message. Determines whether the
        stage workflow runs.

        Returns:
        InterceptionContext:
        - payload: optional payload to continue workflow with (execution skipped).
        - Intercepted: a bool
            - False: not intercepted. Stage proceeds to resolve_inputs and execution.
            - True:  intercepted. Stage halts. Policy is fully responsible for
                what happens next. It may call on_success, on_error, mutate
                metadata, publish to broker, or do nothing at all.
        May mutate message.metadata to annotate the interception reason.
        """
        pass

    @abstractmethod
    def on_running(self, task_id: str, message: Message):
        """
        Notification hook — fired when the task begins execution in the worker.

        Note: runs inside the worker thread or process, not the main process.
        Any callable passed here must be picklable in process-based task queues.
        """
        pass

    @abstractmethod
    def on_done(self, message: Message, envelope: TaskEnvelope[OutputT]):
        """
        Notification hook — fired when the task finishes, regardless of outcome.
        Runs in the main process.
        """
        pass

    @abstractmethod
    def on_success(
        self,
        task_id: str,
        message: Message,
        result: OutputT,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ):
        """
        Notification hook — fired when the handler completes successfully.

        `result` is the OutputT returned by the handler. Use message.metadata
        to determine context — for example, whether this was a cache hit set
        by intercept — and branch persistence or publishing logic accordingly.

        Raising an exception here will be caught by the stage, wrapped in
        PolicyError, and re-fired through on_error.
        """
        pass

    @abstractmethod
    def on_error(
        self,
        message: Message,
        error: Exception,
        broker: BaseMessageBroker | None = None,
    ):
        """
        Called when any failure occurs within the stage lifecycle.

        The error type indicates the failure origin:
        - HandlerError:         task-level failure (handler.run raised). Consider it for requeue
        - InputValidationError: stage-level failure (input coercion failed).
        - PolicyError:          stage-level failure (policy hook raised).
        - StageError:           critical internal stage failure.

        If task-level and stage-level failures require separate handling,
        a future revision may introduce on_task_error / on_stage_error.
        As of now, differentiation is the caller's responsibility via error type.
        """
        pass


class DefaultStagePolicy(
    StagePolicy[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]
):
    """
    No-op implementation of StagePolicy.

    intercept:      always returns False (never intercepts).
    resolve_inputs: returns message.payload, or empty dict if absent.
    All hooks:      no-op.

    Intended as a base class for policies that only need to override
    a subset of hooks, and as the stage fallback when no policy is provided.
    """

    def resolve_inputs(
        self,
        message: Message,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InputT | dict:
        return getattr(message, "payload", {})

    def intercept(
        self,
        message: Message,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InterceptionContext[OutputT]:
        return InterceptionContext(intercepted=False, payload=None)

    def on_running(self, task_id: str, message: Message):
        pass

    def on_done(self, message: Message, envelope: TaskEnvelope[OutputT]):
        pass

    def on_success(
        self,
        task_id: str,
        message: Message,
        result: OutputT,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ):
        pass

    def on_error(
        self,
        message: Message,
        error: Exception,
        broker: BaseMessageBroker | None = None,
    ):
        pass


## builder and factory function
ResolveInputsFn: TypeAlias = Callable[
    [
        Message,
        BaseMessageBroker | None,
        BaseArtifactRepo[ArtifactT, PartNameT] | None,
        Cache[BucketSchemaT] | None,
    ],
    InputT | dict,
]

InterceptFn: TypeAlias = Callable[
    [
        Message,
        BaseArtifactRepo[ArtifactT, PartNameT] | None,
        Cache[BucketSchemaT] | None,
    ],
    InterceptionContext[OutputT],
]

OnRunningFn: TypeAlias = Callable[[Annotated[str, "task_id"], Message], None]

OnDoneFn: TypeAlias = Callable[[Message, TaskEnvelope[OutputT]], None]

OnSuccessFn: TypeAlias = Callable[
    [
        Annotated[str, "task_id"],
        Message,
        OutputT,
        BaseMessageBroker | None,
        BaseArtifactRepo[ArtifactT, PartNameT] | None,
        Cache[BucketSchemaT] | None,
    ],
    None,
]

OnErrorFn: TypeAlias = Callable[[Message, Exception, BaseMessageBroker | None], None]


class StagePolicyBuilder(
    DefaultStagePolicy[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT],
    Generic[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT],
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
        self._intercept_fn: InterceptFn | None = None
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
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]":
        """Override input resolution logic. Returns InputT or dict."""
        self._resolve_inputs_fn = fn
        return self

    def with_intercept(
        self,
        fn: Callable[
            [Message, BaseArtifactRepo | None, Cache[BucketSchemaT] | None],
            InterceptionContext[OutputT],
        ],
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]":
        """Override intercept hook."""
        self._intercept_fn = fn
        return self

    def with_on_running(
        self, fn: Callable[[str, Message], None]
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]":
        """Override on_running hook."""
        self._on_running_fn = fn
        return self

    def with_on_done(
        self, fn: Callable[[Message, TaskEnvelope[OutputT]], None]
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]":
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
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]":
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
    ) -> "StagePolicyBuilder[InputT, OutputT, BucketSchemaT, ArtifactT, PartNameT]":
        """Override on_error hook."""
        self._on_error_fn = fn
        return self

    def resolve_inputs(
        self,
        message: Message,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InputT | dict:
        if self._resolve_inputs_fn:
            return self._resolve_inputs_fn(message, repo, cache)
        return super().resolve_inputs(message, repo, cache)

    def intercept(
        self,
        message: Message,
        broker: BaseMessageBroker | None = None,
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
        cache: Cache[BucketSchemaT] | None = None,
    ) -> InterceptionContext[OutputT]:
        if self._intercept_fn:
            return self._intercept_fn(message, broker, repo, cache)
        return super().intercept(message, broker, repo, cache)

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
        repo: BaseArtifactRepo[ArtifactT, PartNameT] | None = None,
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
