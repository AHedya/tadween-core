import logging
from importlib.metadata import PackageNotFoundError, version

from ._logging import set_logger
from .exceptions import (
    HandlerError,
    InputValidationError,
    PolicyError,
    RoutingError,
    StageError,
    TadweenError,
)

logger = logging.getLogger("tadween")
logger.addHandler(logging.NullHandler())


try:
    __version__ = version("tadween-core")
except PackageNotFoundError:
    __version__ = "0.0.1"


__all__ = [
    "TadweenError",
    "StageError",
    "PolicyError",
    "InputValidationError",
    "HandlerError",
    "RoutingError",
    "set_logger",
    "__version__",
]
