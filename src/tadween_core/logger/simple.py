import logging
import sys
from pathlib import Path

from .formatters import NoStackTraceFormatter

_LOGGER_NAME = "tadween"
_DEFAULT_CONSOLE_FMT = "%(asctime)s:[%(levelname)s] - %(name)s: %(message)s"
_DEFAULT_FILE_FMT = "%(asctime)s:[%(levelname)s] - %(name)s: %(message)s"


class StandardLogger:
    """Plug-and-play logger configuration for the ``tadween`` namespace.

    Configures the root ``tadween`` logger with a console handler and an
    optional file handler.  Safe to use as a context manager, but cleanup
    is a no-op — you can also just instantiate and forget.

    Args:
        level: Logging level (e.g. ``logging.DEBUG``).
        log_path: Optional path to a log file.  When provided, a
            :class:`~logging.FileHandler` is attached.
        console_formatter: Formatter for the console handler.  Defaults to
            :class:`NoStackTraceFormatter` which suppresses stack traces.
        file_formatter: Formatter for the file handler.  Defaults to the
            standard :class:`logging.Formatter`.  Only used when *log_path*
            is provided.
    """

    def __init__(
        self,
        level: int = logging.INFO,
        log_path: str | None = None,
        console_formatter: logging.Formatter | None = None,
        file_formatter: logging.Formatter | None = None,
    ):
        self._level = level
        self._log_path = log_path
        self._console_formatter = console_formatter or NoStackTraceFormatter(
            _DEFAULT_CONSOLE_FMT, datefmt="%H:%M:%S"
        )
        self._file_formatter = file_formatter or logging.Formatter(
            _DEFAULT_FILE_FMT, datefmt="%Y-%m-%d %H:%M:%S"
        )
        self._logger: logging.Logger | None = None

        self._configure()

    @property
    def logger(self) -> logging.Logger:
        """The configured :class:`logging.Logger` instance."""
        return self._logger

    def _configure(self) -> logging.Logger:
        main_logger = logging.getLogger(_LOGGER_NAME)
        main_logger.setLevel(self._level)

        if main_logger.hasHandlers():
            main_logger.handlers.clear()

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(self._console_formatter)
        main_logger.addHandler(stdout_handler)

        if self._log_path:
            path = Path(self._log_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(path, encoding="utf-8")
            file_handler.setFormatter(self._file_formatter)
            main_logger.addHandler(file_handler)

        self._logger = main_logger

    def __enter__(self):
        _ = self.logger
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def set_logger(
    level: int = logging.INFO,
    log_path: str | None = None,
    console_formatter: logging.Formatter | None = None,
    file_formatter: logging.Formatter | None = None,
) -> logging.Logger:
    """Configure the ``tadween`` logger (backward-compatible helper).

    This is a convenience factory that creates a :class:`StandardLogger`
    and returns the underlying :class:`logging.Logger`.

    See :class:`StandardLogger` for parameter details.
    """
    return StandardLogger(
        level=level,
        log_path=log_path,
        console_formatter=console_formatter,
        file_formatter=file_formatter,
    ).logger
