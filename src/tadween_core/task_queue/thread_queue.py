# task_queue/thread_queue.py
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .base_queue import BaseTaskPolicy, BaseTaskQueue


class ThreadTaskQueue(BaseTaskQueue):
    """Task queue using threads - best for I/O-bound tasks."""

    def __init__(
        self,
        name: str | None = None,
        max_workers: int | None = None,
        default_policy: BaseTaskPolicy | None = None,
        logger: logging.Logger | None = None,
        initializer: Callable | None = None,
        initargs: tuple = (),
    ):
        super().__init__(
            name=name, logger=logger, default_policy=default_policy
        )  # CRITICAL: Initialize base class

        self.executor = ThreadPoolExecutor(
            max_workers=max_workers, initializer=initializer, initargs=initargs
        )

    def get_result(self, task_id: str, timeout: float | None = None) -> Any:
        """Get result with thread-specific logging."""
        try:
            result = super().get_result(task_id, timeout)
            self.logger.debug(f"Task {task_id} completed successfully")
            return result
        except Exception as e:
            self.logger.error(f"Task {task_id} failed: {e}")
            raise

    def close(self) -> None:
        """Shutdown gracefully. Blocks the main thread"""
        self.logger.debug(f"Shutting down thread task queue [{self.name}]...")
        self._closed = True
        self.executor.shutdown(wait=True)
        self.logger.debug(f"Thread task queue [{self.name}] shutdown complete")


def thread_task_queue(max_workers: int | None = None) -> ThreadTaskQueue:
    """Create a thread-based task queue"""
    return ThreadTaskQueue(max_workers=max_workers)
