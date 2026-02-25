import logging
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor

from .base_queue import BaseTaskPolicy, BaseTaskQueue


class ProcessTaskQueue(BaseTaskQueue):
    """Task queue using processes - best for CPU-bound tasks."""

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

        self.executor = ProcessPoolExecutor(
            max_workers=max_workers, initializer=initializer, initargs=initargs
        )

    def close(self, force: bool = False) -> None:
        """Shutdown gracefully."""
        self.logger.debug(f"Shutting down process task queue [{self.name}]...")

        self._closed = True
        if force:
            try:
                for pid, process in list(self.executor._processes.items()):
                    process.terminate()
                    self.logger.warning(f"force-terminate workers: {pid}")

            except Exception as e:
                self.logger.warning(f"Failed to force-terminate workers: {e}")
            self.executor.shutdown(wait=False, cancel_futures=True)
        else:
            self.executor.shutdown(wait=True)

        self.logger.debug(f"Process task queue shutdown complete [{self.name}]")
