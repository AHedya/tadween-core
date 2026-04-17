import logging
from importlib.metadata import PackageNotFoundError, version

from .logger import ProcessQueueLogger, QueueLogger, StandardLogger, set_logger
from .throttle import ResourceManager

logger = logging.getLogger("tadween")
logger.addHandler(logging.NullHandler())


try:
    __version__ = version("tadween-core")
except PackageNotFoundError:
    __version__ = "0.1.0"


__all__ = [
    "StandardLogger",
    "QueueLogger",
    "ProcessQueueLogger",
    "set_logger",
    "ResourceManager",
    "__version__",
]
