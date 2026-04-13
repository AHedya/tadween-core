import logging
import logging.handlers
import multiprocessing
import multiprocessing.context
import sys
from multiprocessing.queues import Queue as MpQueue
from pathlib import Path

from .formatters import NoStackTraceFormatter

_LOGGER_NAME = "tadween"
_DEFAULT_CONSOLE_FMT = "%(asctime)s:[%(levelname)s] - %(name)s: %(message)s"
_DEFAULT_FILE_FMT = "%(asctime)s:[%(levelname)s] - %(name)s: %(message)s"


class ProcessQueueLogger:
    """Process-safe, low-latency logger using :class:`logging.handlers.QueueHandler`
    backed by a :class:`multiprocessing.Queue`.

    Log records produced in worker processes are pushed to a
    :class:`multiprocessing.Queue` and dequeued by a
    :class:`~logging.handlers.QueueListener` thread in the main process,
    which dispatches them to the real handlers.

    **Important:** the listener must be stopped before the main process exits
    to ensure buffered records are flushed.  Call :meth:`close` explicitly or
    use as a context manager::

        with ProcessQueueLogger(level=logging.DEBUG) as pql:
            pql.logger.info("main process log")

            # Pass pql.log_queue to worker processes
            with multiprocessing.Process(target=worker, args=(pql.log_queue,)):
                ...

        # listener is stopped automatically

    Forgetting to close the listener does **not** cause severe damage — at
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
        mp_context: A :class:`multiprocessing.context.BaseContext` that
            controls the start method (``'spawn'``, ``'fork'``,
            ``'forkserver'``).  When provided, the queue is created via
            ``mp_context.Queue()``.  Mutually exclusive with *log_queue*.
        log_queue: Optional pre-created :class:`multiprocessing.Queue`.
            Mutually exclusive with *mp_context*.
    """

    def __init__(
        self,
        level: int = logging.INFO,
        log_path: str | None = None,
        console_formatter: logging.Formatter | None = None,
        file_formatter: logging.Formatter | None = None,
        mp_context: multiprocessing.context.BaseContext | None = None,
        log_queue: MpQueue | None = None,
    ):
        if mp_context is not None and log_queue is not None:
            raise ValueError(
                "mp_context and log_queue are mutually exclusive; "
                "provide one or the other, not both."
            )
        self._level = level
        self._log_path = log_path
        self._console_formatter = console_formatter or NoStackTraceFormatter(
            _DEFAULT_CONSOLE_FMT, datefmt="%H:%M:%S"
        )
        self._file_formatter = file_formatter or logging.Formatter(
            _DEFAULT_FILE_FMT, datefmt="%Y-%m-%d %H:%M:%S"
        )
        self._mp_context = mp_context

        if log_queue is not None:
            self._log_queue: MpQueue = log_queue
        elif mp_context is not None:
            self._log_queue = mp_context.Queue()
        else:
            self._log_queue = multiprocessing.Queue()

        self._logger: logging.Logger | None = None
        self._listener: logging.handlers.QueueListener | None = None
        self._closed = False

    @property
    def log_queue(self) -> MpQueue:
        """The :class:`multiprocessing.Queue` used by this logger.

        Pass this to worker processes so they can create their own
        :class:`logging.handlers.QueueHandler` instances.
        """
        return self._log_queue

    @property
    def logger(self) -> logging.Logger:
        """The configured :class:`logging.Logger` instance."""
        if self._logger is None:
            self._configure()
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
