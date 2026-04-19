import logging
from importlib.metadata import PackageNotFoundError, version

from .coord import ResourceManager, StageContextConfig, WorkflowContext
from .logger import ProcessQueueLogger, QueueLogger, StandardLogger, set_logger

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
    "WorkflowContext",
    "StageContextConfig",
    "__version__",
]
