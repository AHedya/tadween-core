import logging
import multiprocessing as mp
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from multiprocessing.context import BaseContext

from .base_queue import BaseTaskPolicy, BaseTaskQueue


def _resolve_mp_context(mp_context: BaseContext | str | None) -> BaseContext:
    """
    Resolve an mp_context parameter into an actual multiprocessing BaseContext.
    multiprocessing.get_context() (platform default) on failure.
    """
    if isinstance(mp_context, str):
        try:
            return mp.get_context(mp_context)
        except Exception:
            return mp.get_context()
    elif mp_context is None:
        # default to 'spawn' for safety in multi-threaded environments
        try:
            return mp.get_context("spawn")
        except Exception:
            return mp.get_context()
    else:
        return mp_context


class ProcessTaskQueue(BaseTaskQueue):
    """Task queue using processes - best for CPU-bound tasks."""

    def __init__(
        self,
        name: str | None = None,
        mp_context: BaseContext | str | None = None,
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
        mp_context = _resolve_mp_context(mp_context)
        self.executor = ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=initializer,
            initargs=initargs,
            mp_context=mp_context,
        )

    def close(self, force: bool = False) -> None:
        """Shutdown gracefully."""
        self.logger.debug(f"Shutting down process task queue [{self.name}]...")

        self._closed = True
        if force:
            try:
                # concurrent.futures uses internal _processes attribute
                processes = getattr(self.executor, "_processes", {})
                for pid, process in list(processes.items()):
                    process.terminate()
                    self.logger.warning(f"force-terminate worker: {pid}")

            except Exception as e:
                self.logger.warning(f"Failed to force-terminate workers: {e}")
            self.executor.shutdown(wait=False, cancel_futures=True)
        else:
            self.executor.shutdown(wait=True)

        self.logger.debug(f"Process task queue shutdown complete [{self.name}]")
