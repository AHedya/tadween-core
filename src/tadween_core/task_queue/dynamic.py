# task_queue/dynamic_queue.py
from collections.abc import Callable
from enum import Enum
from typing import Any

from .base_queue import BaseTaskQueue, TaskStatus
from .process_queue import ProcessTaskQueue
from .thread_queue import ThreadTaskQueue


class ExecutorType(Enum):
    THREAD = "thread"
    PROCESS = "process"
    AUTO = "auto"


class DynamicTaskQueue(BaseTaskQueue):
    """
    Dynamic task queue that can use either threads or processes.
    Allows per-task selection of executor type.
    """

    def __init__(
        self,
        name: str | None = None,
        default_executor: ExecutorType | None = None,
        max_thread_workers: int | None = None,
        max_process_workers: int | None = None,
    ):
        self.name = name or f"Dynamic-{self.__class__.__name__}-{id(self):x}"

        self.default_executor = default_executor or ExecutorType.THREAD
        self.thread_queue = ThreadTaskQueue(max_workers=max_thread_workers)
        self.process_queue = ProcessTaskQueue(max_workers=max_process_workers)

        # Track which queue owns which task
        self._task_registry: dict[str, BaseTaskQueue] = {}

    def submit(
        self,
        fn: Callable[..., Any],
        *args,
        executor: ExecutorType | None = None,
        **kwargs,
    ) -> str:
        """
        Submit a task with optional executor override.

        Args:
            fn: Function to execute
            *args: Positional arguments
            executor: Override default executor (THREAD or PROCESS)
            **kwargs: Keyword arguments
        """
        executor_type = executor or self.default_executor

        if executor_type == ExecutorType.THREAD:
            task_id = self.thread_queue.submit(fn, *args, **kwargs)
            self._task_registry[task_id] = self.thread_queue
        elif executor_type == ExecutorType.PROCESS:
            task_id = self.process_queue.submit(fn, *args, **kwargs)
            self._task_registry[task_id] = self.process_queue
        else:
            raise ValueError(f"Unsupported executor type: {executor_type}")

        return task_id

    def get_result(self, task_id: str, timeout: float | None = None) -> Any:
        """Get result from appropriate queue"""
        if task_id not in self._task_registry:
            raise ValueError(f"Unknown task ID: {task_id}")

        queue = self._task_registry[task_id]
        result = queue.get_result(task_id, timeout=timeout)

        # Clean up registry
        del self._task_registry[task_id]

        return result

    def get_status(self, task_id: str) -> TaskStatus:
        """Get status from appropriate queue"""
        if task_id not in self._task_registry:
            raise ValueError(f"Unknown task ID: {task_id}")

        queue = self._task_registry[task_id]
        return queue.get_status(task_id)

    def cancel(self, task_id: str) -> bool:
        """Cancel task in appropriate queue"""
        if task_id not in self._task_registry:
            return False

        queue = self._task_registry[task_id]
        cancelled = queue.cancel(task_id)

        if cancelled:
            del self._task_registry[task_id]

        return cancelled

    def close(self) -> None:
        """Close both queues"""
        self.thread_queue.close()
        self.process_queue.close()
