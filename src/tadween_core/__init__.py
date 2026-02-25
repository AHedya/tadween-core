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

try:
    __version__ = version("tadween-core")
except PackageNotFoundError:
    __version__ = "0.0.0"


__all__ = [
    "TadweenArtifact",
    "TadweenError",
    "StageError",
    "PolicyError",
    "InputValidationError",
    "HandlerError",
    "RoutingError",
    "__version__",
]

import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s:[%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
