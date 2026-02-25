from abc import ABC, abstractmethod
from concurrent.futures import Future

from tadween_core.task_queue.base import T, TaskEnvelope


class BaseTaskPolicy(ABC):
    """
    Lifecycle policy for task execution.
    Handles task-level concerns: timing, logging, resource tracking, etc.
    Policy object must be static: type[BaseTaskPolicy]. sharing instance could be problematic in multiprocessing
    """

    @abstractmethod
    def on_submit(task_id: str) -> None:
        """Called when task is submitted to queue."""
        pass

    @abstractmethod
    def on_running(task_id: str) -> None:
        """Called when task execution begins (not queued anymore)."""
        pass

    @abstractmethod
    def on_done(task_id: str, future: Future[TaskEnvelope[T]]) -> None:
        """Called when task completes (success or failure)."""
        pass
