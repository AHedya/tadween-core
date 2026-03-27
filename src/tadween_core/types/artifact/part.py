import io
import pickle
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


class ArtifactPart(BaseArtifactPart):
    """Default implementation of :class:`BaseArtifactPart` using msgpack."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def serialize(self) -> bytes:
        return msgpack.packb(self.model_dump(mode="python"), use_bin_type=True)

    @classmethod
    def deserialize(cls, data: bytes, strict_map_key: bool = True) -> Self:
        return cls.model_validate(msgpack.unpackb(data, strict_map_key=strict_map_key))


class PicklePart(BaseArtifactPart):
    """Pickle-based artifact part for arbitrary Python objects.

    SECURITY WARNING: Pickle can execute arbitrary code during deserialization.
    Only use with trusted data sources. Never deserialize untrusted pickle data.

    Unlike :class:`ArtifactPart`, this requires no custom validators for complex types.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        return pickle.loads(data)


# utility for serialization/validation


def ser_ndarray(value: np.ndarray) -> bytes:
    buf = io.BytesIO()
    np.save(buf, value, allow_pickle=False)
    return buf.getvalue()


def val_ndarray(value: bytes):
    return np.load(io.BytesIO(value), allow_pickle=False)
