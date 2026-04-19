import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Self


class WorkflowContext:
    """
    Global context for managing workflow state and logical synchronization.

    Provides event channels for stages to wait on and notify each other,
    allowing complex backpressure and deferral logic without coupling stages.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._conditions: dict[str, threading.Condition] = {}
        self._is_shutdown = False
        self.state: dict[str, Any] = {}

    def _get_condition(self, event_name: str) -> threading.Condition:
        """Get or create a condition for a specific event channel."""
        with self._lock:
            if event_name not in self._conditions:
                self._conditions[event_name] = threading.Condition(self._lock)
            return self._conditions[event_name]

    def wait_for(
        self,
        event_name: str,
        predicate: Callable[[Self], bool],
        poll_interval: float = 1.0,
        timeout: float | None = None,
        update_on_acquire: dict[str, int] | None = None,
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
            predicate: A function ``(WorkflowContext) -> bool``. Blocks if True.
            poll_interval: Sleep time between predicate checks if no notification is received.
            timeout: Maximum time to wait.
            update_on_acquire: Optional state updates to apply atomically after the wait clears.

        Raises:
            TimeoutError: Deferral timeout
        """
        start_time = time.monotonic()
        condition = self._get_condition(event_name)

        with condition:
            while predicate(self):
                if self._is_shutdown:
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

            if update_on_acquire:
                for key, delta in update_on_acquire.items():
                    self.state[key] = self.state.get(key, 0) + delta

    def notify(self, events: list[str] | str | None = None, n: int = 0) -> None:
        """
        Wakes up all or a number of threads waiting on a specific event channel.
        """
        target: list[threading.Condition] = []

        if events is None:
            with self._lock:
                target = list(self._conditions.values())
        elif isinstance(events, str):
            target = [self._get_condition(events)]
        else:
            for name in events:
                target.append(self._get_condition(name))

        for condition in target:
            with condition:
                if n <= 0:
                    condition.notify_all()
                else:
                    condition.notify(n=n)

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

    def apply_state(self, updates: dict[str, int]) -> None:
        with self._lock:
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


@dataclass(slots=True)
class StageContextConfig:
    """
    Configuration for a stage's logical deferral and synchronization.

    Attributes:
        context: The shared WorkflowContext.
        defer_predicate: Pure function that blocks the stage if it returns True.
        defer_event: The logical channel to wait on (e.g., "stash_limit").
        defer_timeout: Max time to wait before failing the message.
        defer_poll_interval: Polling frequency for the predicate.
        notify_events: Channels to wake up when this stage finishes a task.
        n_notify: Number of threads to wake up (0 = all).
        defer_state_update: State changes applied ATOMICALLY after defer_predicate clears.
        done_state_update: State changes applied when this stage finishes a task.
    """

    context: WorkflowContext | None = None
    defer_predicate: Callable[[WorkflowContext], bool] | None = None
    defer_event: str = "default"
    defer_timeout: float | None = None
    defer_poll_interval: float = 1.0
    notify_events: list[str] = field(default_factory=list)
    n_notify: int = 0
    defer_state_update: dict[str, int] = field(default_factory=dict)
    done_state_update: dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if not self.notify_events:
            self.notify_events = [self.defer_event]
