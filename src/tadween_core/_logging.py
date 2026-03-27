import logging
import sys
from pathlib import Path


def set_logger(level=logging.INFO, log_path=None):
    """
    Configures the 'tadween_core' logger.

    Args:
        level: Logging level (e.g., logging.DEBUG)
        log_path: Optional string or Path to a log file.
    """
    main_logger = logging.getLogger("tadween")
    main_logger.setLevel(level)

    if main_logger.hasHandlers():
        main_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s:[%(levelname)s] - %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    main_logger.addHandler(stdout_handler)

    if log_path:
        path = Path(log_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        main_logger.addHandler(file_handler)

    return main_logger
