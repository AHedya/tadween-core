import logging
import logging.handlers
import queue
from pathlib import Path

from ...utils import FileMode, reserve_path
from ..schemas import (  # noqa
    RUNTIME_HEADER,
    RUNTIME_SEP,
    RuntimeMetadata,
    RuntimeSample,
)


class RuntimeCollector:
    """
    A collector that logs runtime metrics.
    It uses a QueueHandler/QueueListener pattern to offload I/O to a separate thread.
    """

    def __init__(
        self,
        log_path: str | Path = "logs/tasks_timing.log",
        file_mode: FileMode = "append",
        start_session: bool = True,
        title: str = "tasks-runtime-metric",
        description: str | None = None,
    ):
        self._log_path = Path(log_path)
        # Ensure path is reserved/handled by util
        self._log_path = reserve_path(
            self._log_path.parent, self._log_path.name, file_mode
        )

        self._logger: logging.Logger | None = None
        self._queue: queue.Queue | None = None
        self._listener: logging.handlers.QueueListener | None = None

        self._setup_logger()
        if start_session:
            self.start_session(title, description)

    def log(self, message: str | RuntimeSample):
        if isinstance(message, RuntimeSample):
            message = message.to_sample_record()
        self._logger.info(message)

    def stop(self):
        """Stops the listener to flush the queue. Call during shutdown."""
        if self._listener:
            self._listener.stop()
            self._listener = None

    def _setup_logger(self):
        self._logger = logging.getLogger(f"runtime-metric-{id(self)}")

        if self._logger.handlers:
            return

        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False

        # Setup Queue
        self._queue = queue.Queue()
        qh = logging.handlers.QueueHandler(self._queue)
        self._logger.addHandler(qh)

        fh = logging.FileHandler(self._log_path, mode="a", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(message)s"))

        # Setup Listener
        self._listener = logging.handlers.QueueListener(self._queue, fh)
        self._listener.start()

    def start_session(self, title: str, description: str | None = None):
        self.log(
            f"{RUNTIME_HEADER}{RUNTIME_SEP}{RuntimeMetadata(title, description).to_json()}"
        )


# Singleton instance holder
_global_collector: RuntimeCollector | None = None


def get_runtime_collector(
    log_path: str | Path = "logs/tasks_timing.log",
    file_mode: FileMode = "append",
) -> RuntimeCollector:
    global _global_collector

    if _global_collector is None:
        _global_collector = RuntimeCollector(log_path=log_path, file_mode=file_mode)

    return _global_collector


def stop_analytics():
    global _global_collector
    if _global_collector:
        _global_collector.stop()
        _global_collector = None
