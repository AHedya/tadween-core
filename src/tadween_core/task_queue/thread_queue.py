import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .base_queue import BaseTaskPolicy, BaseTaskQueue


class ThreadTaskQueue(BaseTaskQueue):
    """Task queue using threads - best for I/O-bound tasks."""

    def __init__(
        self,
        name: str | None = None,
        max_workers: int | None = None,
        default_policy: BaseTaskPolicy | None = None,
        retain_results: bool = False,
        logger: logging.Logger | None = None,
        initializer: Callable | None = None,
        initargs: tuple = (),
    ):
        super().__init__(
            name=name,
            logger=logger,
            default_policy=default_policy,
            retain_results=retain_results,
        )  # CRITICAL: Initialize base class

        self.executor = ThreadPoolExecutor(
            max_workers=max_workers, initializer=initializer, initargs=initargs
        )

    def close(self) -> None:
        """Shutdown gracefully. Blocks the main thread"""
        self.logger.debug(f"Shutting down thread task queue [{self.name}]...")
        self._closed = True
        self.executor.shutdown(wait=True)
        self.logger.debug(f"Thread task queue [{self.name}] shutdown complete")
