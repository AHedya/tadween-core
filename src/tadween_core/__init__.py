from importlib.metadata import PackageNotFoundError, version

from .exceptions import (
    HandlerError,
    InputValidationError,
    PolicyError,
    RoutingError,
    StageError,
    TadweenError,
)
from .types.artifact.tadween import TadweenArtifact
from .utils import set_logger

try:
    __version__ = version("tadween-core")
except PackageNotFoundError:
    __version__ = "0.0.1"


__all__ = [
    "TadweenArtifact",
    "TadweenError",
    "StageError",
    "PolicyError",
    "InputValidationError",
    "HandlerError",
    "RoutingError",
    "set_logger",
    "__version__",
]
