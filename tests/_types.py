from pathlib import Path

from pydantic import BaseModel

from tadween_core.types.artifact.base import ArtifactPart, BaseArtifact


class ArtifactTestPart(ArtifactPart):
    content: str = "test-content"


class ArtifactTestMetadata(BaseModel):
    checksum: str | None = None
    audio_path: Path | None = None
    duration: float | None = None


class ArtifactTest(BaseArtifact):
    # Added to satisfy current SqliteRepo schema
    current_stage: str = "created"
    error_stage: str | None = None

    metadata: ArtifactTestMetadata | None = None
    part_a: ArtifactTestPart | None = None
    part_b: ArtifactTestPart | None = None

    @classmethod
    def get_part_map(cls) -> dict[str, type[ArtifactPart]]:
        return {"part_a": ArtifactTestPart, "part_b": ArtifactTestPart}

    @classmethod
    def part_names(cls) -> frozenset[str]:
        return frozenset(["part_a", "part_b"])
