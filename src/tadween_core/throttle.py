import threading

from .exceptions import ResourceError


class ResourceManager:
    """
    Thread-safe resource pool that controls concurrent access to finite resources.

    Stages declare their resource *demands* (e.g. ``{"cuda": 1, "RAM_MB": 500}``)
    and the manager ensures that no more than the configured *capacity* is
    allocated at any given time.  ``acquire`` blocks the caller (the stage's
    collector thread) until all demanded units are available atomically;
    ``release`` returns them to the pool.

    Typical usage::

        manager = ResourceManager(resources={"cuda": 1, "RAM_MB": 2048})

        # Inside the collector thread, before submitting to the task queue:
        manager.acquire({"cuda": 1})

        # Inside the on_done callback, after the task finishes:
        manager.release({"cuda": 1})
    """

    def __init__(self, resources: dict[str, float]):
        if not resources:
            raise ValueError("ResourceManager requires at least one resource.")

        for name, capacity in resources.items():
            if capacity <= 0:
                raise ValueError(
                    f"Resource '{name}' capacity must be positive, got {capacity}."
                )

        self._capacity: dict[str, float] = dict(resources)
        self._available: dict[str, float] = dict(resources)
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._is_shutdown = False

    def acquire(self, demands: dict[str, float]) -> None:
        """
        Block until *all* demanded resources are available, then reserve them.

        The reservation is atomic: either every resource is acquired or the
        caller waits.  This prevents partial allocation (e.g. holding RAM while
        waiting for CUDA) that could cause deadlocks.

        Args:
            demands: Mapping of resource name → units required.

        Raises:
            ResourceError: If the manager has been shut down.
            ValueError: If a demanded resource is unknown.
        """
        self._validate_demands(demands)

        with self._condition:
            while not self._can_acquire(demands):
                if self._is_shutdown:
                    raise ResourceError(
                        "ResourceManager is shut down; cannot acquire resources."
                    )
                self._condition.wait()

            if self._is_shutdown:
                raise ResourceError(
                    "ResourceManager is shut down; cannot acquire resources."
                )

            for name, units in demands.items():
                self._available[name] -= units

    def release(self, demands: dict[str, float]) -> None:
        """
        Return previously acquired units to the pool and wake waiting threads.

        Args:
            demands: Mapping of resource name → units to return.  Must match
                    a prior ``acquire`` call.

        Raises:
            ValueError: If a resource is unknown or release would exceed capacity.
        """
        self._validate_demands(demands)

        with self._condition:
            for name, units in demands.items():
                new_val = self._available[name] + units
                if new_val > self._capacity[name]:
                    raise ValueError(
                        f"Release of {units} {name} would exceed capacity "
                        f"({new_val} > {self._capacity[name]})."
                    )
                self._available[name] = new_val

            self._condition.notify_all()

    def shutdown(self) -> None:
        """
        Signal shutdown and wake all waiting threads.

        Waiting ``acquire`` calls will raise ``ResourceError``.
        """
        with self._condition:
            self._is_shutdown = True
            self._condition.notify_all()

    @property
    def available(self) -> dict[str, float]:
        """Snapshot of currently available units per resource."""
        with self._lock:
            return dict(self._available)

    @property
    def capacity(self) -> dict[str, float]:
        """Snapshot of total capacity per resource."""
        return dict(self._capacity)

    @property
    def is_shutdown(self) -> bool:
        return self._is_shutdown

    def _can_acquire(self, demands: dict[str, float]) -> bool:
        for name, units in demands.items():
            if self._available[name] < units:
                return False
        return True

    def _validate_demands(self, demands: dict[str, float]) -> None:
        if not demands:
            return

        for name, units in demands.items():
            if name not in self._capacity:
                raise ValueError(
                    f"Unknown resource '{name}'. "
                    f"Available resources: {list(self._capacity.keys())}."
                )
            if units <= 0:
                raise ValueError(f"Demand for '{name}' must be positive, got {units}.")
            if units > self._capacity[name]:
                raise ValueError(
                    f"Demand for '{name}' ({units}) exceeds total capacity ({self._capacity[name]})."
                )
