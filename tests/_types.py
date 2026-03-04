import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from tadween_core.types.artifact.base import ArtifactPart, BaseArtifact, RootModel


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


class ArtifactTest(BaseArtifact):
    root: ArtifactRoot
    metadata: ArtifactTestMetadata
    part_a: ArtifactTestPart
    part_b: ArtifactTestPart


part_names = Literal["part_a", "part_b"]
