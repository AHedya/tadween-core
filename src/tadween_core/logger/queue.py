import logging
import logging.handlers
import queue
import sys
from pathlib import Path

from .formatters import NoStackTraceFormatter

_LOGGER_NAME = "tadween"
_DEFAULT_CONSOLE_FMT = "%(asctime)s:[%(levelname)s] - %(name)s: %(message)s"
_DEFAULT_FILE_FMT = "%(asctime)s:[%(levelname)s] - %(name)s: %(message)s"


class QueueLogger:
    """Thread-based, low-latency logger using a :class:`logging.handlers.QueueHandler`.

    **Important:** the listener must be stopped before the process exits to
    ensure buffered records are flushed.  Call :meth:`close` explicitly or
    use as a context manager::

        with QueueLogger(level=logging.DEBUG) as ql:
            ql.logger.info("fast, non-blocking")

        # listener is stopped automatically

    Forgetting to close the listener does **not** cause severe damage, at
    worst, the last few buffered records may be lost. The listener thread is
    a daemon so it will not prevent process exit.

    Args:
        level: Logging level (e.g. ``logging.DEBUG``).
        log_path: Optional path to a log file.
        console_formatter: Formatter for the console handler.  Defaults to
            :class:`NoStackTraceFormatter`.
        file_formatter: Formatter for the file handler.  Defaults to the
            standard :class:`logging.Formatter`.  Only used when *log_path*
            is provided.
        log_queue: Optional :class:`queue.Queue` instance.  When *None* a
            new unbounded queue is created automatically.
    """

    def __init__(
        self,
        level: int = logging.INFO,
        log_path: str | None = None,
        console_formatter: logging.Formatter | None = None,
        file_formatter: logging.Formatter | None = None,
        log_queue: queue.Queue | None = None,
    ):
        self._level = level
        self._log_path = log_path
        self._console_formatter = console_formatter or NoStackTraceFormatter(
            _DEFAULT_CONSOLE_FMT, datefmt="%H:%M:%S"
        )
        self._file_formatter = file_formatter or logging.Formatter(
            _DEFAULT_FILE_FMT, datefmt="%Y-%m-%d %H:%M:%S"
        )
        self._log_queue = log_queue or queue.Queue()
        self._logger: logging.Logger | None = None
        self._listener: logging.handlers.QueueListener | None = None
        self._closed = False

        self._configure()

    @property
    def log_queue(self) -> queue.Queue:
        """The :class:`queue.Queue` used by this logger."""
        return self._log_queue

    @property
    def logger(self) -> logging.Logger:
        """The configured :class:`logging.Logger` instance."""
        return self._logger

    def _configure(self) -> None:
        main_logger = logging.getLogger(_LOGGER_NAME)
        main_logger.setLevel(self._level)

        if main_logger.hasHandlers():
            main_logger.handlers.clear()

        handlers: list[logging.Handler] = []

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(self._console_formatter)
        handlers.append(stdout_handler)

        if self._log_path:
            path = Path(self._log_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(path, encoding="utf-8")
            file_handler.setFormatter(self._file_formatter)
            handlers.append(file_handler)

        queue_handler = logging.handlers.QueueHandler(self._log_queue)
        main_logger.addHandler(queue_handler)

        self._listener = logging.handlers.QueueListener(
            self._log_queue,
            *handlers,
            respect_handler_level=True,
        )
        self._listener.start()

        self._logger = main_logger

    def close(self) -> None:
        """Stop the listener thread and flush buffered records.

        Safe to call multiple times; subsequent calls are no-ops.
        """
        if self._closed:
            return
        self._closed = True
        if self._listener is not None:
            self._listener.stop()

    def __enter__(self):
        _ = self.logger
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
