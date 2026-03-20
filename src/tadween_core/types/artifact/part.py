import io
from abc import ABC, abstractmethod
from typing import Self

import msgpack
import numpy as np
from pydantic import BaseModel, ConfigDict


class BaseArtifactPart(BaseModel, ABC):
    """Base class for all heavy artifact parts.
    Inherit from this class to mark a field as a lazy-loaded part.

    You'd need to implement custom serialization/validation logic if you use arbitrary/custom types.
    """

    @abstractmethod
    def serialize(self) -> bytes: ...

    @classmethod
    @abstractmethod
    def deserialize(cls, data: bytes) -> Self: ...


class ArtifactPart(BaseArtifactPart, ABC):
    """Default implementation of :class:`BaseArtifactPart`"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def serialize(self) -> bytes:
        return msgpack.packb(self.model_dump(mode="python"), use_bin_type=True)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        return cls.model_validate(msgpack.unpackb(data))


# utility for serialization/validation


def ser_ndarray(value: np.ndarray) -> bytes:
    buf = io.BytesIO()
    np.save(buf, value, allow_pickle=False)
    return buf.getvalue()


def val_ndarray(value: bytes):
    return np.load(io.BytesIO(value), allow_pickle=False)
