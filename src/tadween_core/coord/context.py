import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Self

EVENT_ARTIFACT_DONE = "__tadween.artifact.done"


class WorkflowContext:
    """
    Global context for managing workflow state and logical synchronization.

    Acts as a shared state container and a Reactive Event Bus. Stages can wait on
    conditions or register callbacks to react to specific events (e.g., artifact completion).
    """

    def __init__(self, logger: logging.Logger | None = None):
        self._lock = threading.RLock()
        self._conditions: dict[str, threading.Condition] = {}
        self._callbacks: dict[str, list[Callable]] = {}
        self._is_shutdown = False
        self.state: dict[str, Any] = {}
        self.logger = logger or logging.getLogger("tadween.coord.context")

    def _get_condition(self, event_name: str) -> threading.Condition:
        """Get or create a condition for a specific event channel."""
        with self._lock:
            if event_name not in self._conditions:
                self._conditions[event_name] = threading.Condition(self._lock)
            return self._conditions[event_name]

    def wait_for(
        self,
        event_name: str,
        predicate: Callable[[Self, dict], bool],
        metadata: dict | None = None,
        poll_interval: float | None = 1.0,
        timeout: float | None = None,
        update_on_acquire: dict[str, int] | Callable[[Self, dict], None] | None = None,
    ) -> None:
        """
        Blocks the calling thread as long as the predicate returns True.

        Once the predicate evaluates to False, atomically applies ``update_on_acquire``
        while the internal lock is still held. This prevents race conditions and
        spurious wakeup side-effects.

        .. warning::
            The ``predicate`` function MUST be pure (read-only). Do not mutate
            state inside the predicate, as it may be called multiple times.

        Args:
            event_name: The channel to wait on.
            predicate: A function ``(WorkflowContext, dict) -> bool``. Blocks if True.
            metadata: Generic metadata dictionary passed to the predicate and hook.
            poll_interval: Sleep time between predicate checks if no notification is received.
            timeout: Maximum time to wait.
            update_on_acquire: Optional state updates (dict) or hook function ``(WorkflowContext, dict) -> None`` to apply atomically after the wait clears.

        Raises:
            TimeoutError: Deferral timeout
        """
        metadata = metadata or {}
        start_time = time.monotonic()
        condition = self._get_condition(event_name)

        with condition:
            while predicate(self, metadata):
                if self._is_shutdown:
                    # TODO: Create and use context error instead of runtime to match resource manager
                    raise RuntimeError("WorkflowContext is shut down.")
                if timeout is not None:
                    remaining = timeout - (time.monotonic() - start_time)
                    if remaining <= 0:
                        raise TimeoutError(
                            f"Deferral timeout of {timeout}s exceeded on event '{event_name}'."
                        )

                    wait_time = min(poll_interval, remaining)
                else:
                    wait_time = poll_interval

                condition.wait(timeout=wait_time)

            if update_on_acquire is not None:
                if callable(update_on_acquire):
                    update_on_acquire(self, metadata)
                else:
                    for key, delta in update_on_acquire.items():
                        self.state[key] = self.state.get(key, 0) + delta

    def notify(
        self, events: list[str] | str | None = None, n: int = 0, **kwargs
    ) -> None:
        """
        Wakes up all or a number of threads waiting on a specific event channel.
        and invokes any registered callbacks for those events.

        Args:
            events: Events (channels) to notify. `None` means notify *all*
            n: number of waiting threads on this channel to wake up. `<=0` means all waiting threads.
            kwargs: keyword args passed to the registered callbacks for this event.
        """
        target: list[threading.Condition] = []
        target_events: list[str] = []

        if events is None:
            with self._lock:
                target = list(self._conditions.values())
                target_events = list(self._conditions.keys()) + list(
                    self._callbacks.keys()
                )
                target_events = list(set(target_events))
        elif isinstance(events, str):
            target = [self._get_condition(events)]
            target_events = [events]
        else:
            target_events = events
            for name in events:
                target.append(self._get_condition(name))

        for condition in target:
            with condition:
                if n <= 0:
                    condition.notify_all()
                else:
                    condition.notify(n=n)

        # Invoke callbacks outside the lock
        for event_name in target_events:
            callbacks = []
            with self._lock:
                callbacks = list(self._callbacks.get(event_name, []))
            for cb in callbacks:
                try:
                    cb(**kwargs)
                except Exception as e:
                    self.logger.error(
                        f"Callback for event '{event_name}' failed: {e}", exc_info=True
                    )

    def on(self, event_name: str, callback: Callable):
        """
        Registers a callback to be executed when an event is notified.

        Args:
            event_name: The name of the event to listen for.
            callback: A callable invoked as `callback(**kwargs)`.
        """
        with self._lock:
            if event_name not in self._callbacks:
                self._callbacks[event_name] = []
            self._callbacks[event_name].append(callback)

    def track_artifact_progress(self, artifact_id: str, delta: int, **kwargs) -> int:
        """
        Atomically updates task count for an artifact and cleans up state if completed.

        If an artifact is distributed across many stages, the artifact count is greater than *one* (>=1).\n
        An artifact is considered *done* when its count reaches *zero* (<=0).
        This quiescence-like mechanism is handled automatically by `WorkflowRoutingPolicy`.

        Args:
            artifact_id: The unique identifier for the artifact.
            delta: The net change in task count (e.g., num_children - 1).
            **kwargs: Additional metadata passed to the 'artifact_done' event.

        Returns:
            The new task count.
        """
        should_notify = False
        new_count = 0
        key = f"_artifacts:{artifact_id}"

        with self._lock:
            new_count = self.state.get(key, 0) + delta
            if new_count <= 0:
                should_notify = True
                # Cleanup state to prevent memory leaks
                self.state.pop(key, None)
            else:
                self.state[key] = new_count

        if should_notify:
            # Trigger notification outside the lock to prevent deadlocks
            self.notify(EVENT_ARTIFACT_DONE, artifact_id=artifact_id, **kwargs)
        return new_count

    def increment(self, key: str, delta: int = 1) -> int:
        with self._lock:
            self.state[key] = self.state.get(key, 0) + delta
            return self.state[key]

    def decrement(self, key: str, delta: int = 1) -> int:
        with self._lock:
            self.state[key] = self.state.get(key, 0) - delta
            return self.state[key]

    def state_get(self, key: str, default: int = 0) -> int:
        with self._lock:
            return self.state.get(key, default)

    def apply_state(
        self,
        updates: dict[str, int] | Callable[[Self, dict], None],
        metadata: dict | None = None,
    ) -> None:
        metadata = metadata or {}
        with self._lock:
            if callable(updates):
                updates(self, metadata)
            else:
                for key, delta in updates.items():
                    self.state[key] = self.state.get(key, 0) + delta

    def shutdown(self) -> None:
        """
        Signals shutdown and wakes up all waiting threads across all channels.
        """
        with self._lock:
            self._is_shutdown = True
            conditions = list(self._conditions.values())

        # Notify outside the dict_lock to prevent deadlocks
        for condition in conditions:
            with condition:
                condition.notify_all()

    @property
    def is_shutdown(self) -> bool:
        return self._is_shutdown

    def on_artifact_done(self, callback: Callable):
        """
        Registers a callback to be executed when an artifact finishes processing.

        The callback is invoked as `callback(artifact_id=..., **kwargs)`.
        """
        self.on(EVENT_ARTIFACT_DONE, callback)


@dataclass(slots=True)
class StageContextConfig:
    """
    Configuration for a stage's logical deferral and synchronization.

    Attributes:
        context: The shared WorkflowContext.
        defer_predicate: Pure function that blocks the stage if it returns True.
            Receives (WorkflowContext, metadata_dict).
        defer_event: The logical channel to wait on (e.g., "stash_limit").
        defer_timeout: Max time to wait before failing the message.
        defer_poll_interval: Polling frequency for the predicate.
        notify_events: Channels to wake up when this stage finishes a task.
        n_notify: Number of threads to wake up (0 = all).
        defer_state_update: State changes applied ATOMICALLY after defer_predicate clears.
            Can be a dict or a Callable[[WorkflowContext, metadata_dict], None].
        done_state_update: State changes applied when this stage finishes a task.
            Can be a dict or a Callable[[WorkflowContext, metadata_dict], None].
        rollback_state_update: Optional state changes applied if the stage fails to enqueue the task.
            If None and defer_state_update is a dict, it is automatically derived by inverting values.
    """

    context: WorkflowContext | None = None
    defer_predicate: Callable[[WorkflowContext, dict], bool] | None = None
    defer_event: str = "default"
    defer_timeout: float | None = None
    defer_poll_interval: float = 1.0
    notify_events: list[str] = field(default_factory=list)
    n_notify: int = 0
    defer_state_update: dict[str, int] | Callable[[WorkflowContext, dict], None] = (
        field(default_factory=dict)
    )
    done_state_update: dict[str, int] | Callable[[WorkflowContext, dict], None] = field(
        default_factory=dict
    )
    rollback_state_update: (
        dict[str, int] | Callable[[WorkflowContext, dict], None] | None
    ) = None

    def __post_init__(self):
        if not self.notify_events:
            self.notify_events = [self.defer_event]
