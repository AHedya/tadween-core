import random
import time
from pathlib import Path
from typing import Any, Literal

import numpy as np
from pydantic import (
    BaseModel,
    Field,
    SerializationInfo,
    field_serializer,
    field_validator,
)

from tadween_core.types.artifact import ArtifactPart, BaseArtifact, RootModel
from tadween_core.types.artifact.part import ser_ndarray, val_ndarray


def generate_random_array(size) -> np.ndarray:
    return np.array([random.gauss(0, 1) for _ in range(size)])


class ArtifactTestPart(ArtifactPart):
    content: str = "test-content"
    result: dict = {"res1": "this is a very huge result"}


class ArtifactTestMetadata(BaseModel):
    checksum: str | None = None
    file_path: Path | None = None
    duration: float | None = None


class ArtifactRoot(RootModel):
    stage: str
    created_at: float = Field(default_factory=time.time)


class AudioPart(ArtifactPart):
    voice: np.ndarray = generate_random_array(100)

    @field_serializer("voice")
    def serialize_voice(
        self, value: np.ndarray, info: SerializationInfo
    ) -> list | bytes:
        if info.mode == "python":
            return ser_ndarray(value)
        elif info.mode == "json":
            return value.tolist()

    @field_validator("voice", mode="before")
    @classmethod
    def validate_voice(cls, value: Any) -> np.ndarray:
        if isinstance(value, list):
            return np.array(value)
        elif isinstance(value, bytes):
            return val_ndarray(value)
        else:
            return value


class ArtifactTest(BaseArtifact):
    root: ArtifactRoot
    metadata: ArtifactTestMetadata
    part_a: ArtifactTestPart
    part_b: ArtifactTestPart
    audio: AudioPart


part_names = Literal["part_a", "part_b", "audio"]
