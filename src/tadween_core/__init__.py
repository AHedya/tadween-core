import logging
from importlib.metadata import PackageNotFoundError, version

from .exceptions import (
    HandlerError,
    InputValidationError,
    PolicyError,
    RoutingError,
    StageError,
    TadweenError,
)
from .logger import ProcessQueueLogger, QueueLogger, StandardLogger, set_logger

logger = logging.getLogger("tadween")
logger.addHandler(logging.NullHandler())


try:
    __version__ = version("tadween-core")
except PackageNotFoundError:
    __version__ = "0.1.0"


__all__ = [
    "TadweenError",
    "StageError",
    "PolicyError",
    "InputValidationError",
    "HandlerError",
    "RoutingError",
    "StandardLogger",
    "QueueLogger",
    "ProcessQueueLogger",
    "set_logger",
    "__version__",
]
