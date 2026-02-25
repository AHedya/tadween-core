import shutil
import tempfile
from pathlib import Path

import pytest
from pydantic import BaseModel

from tadween_core.repo import SqliteRepo
from tadween_core.repo.json import FsJsonRepo
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


@pytest.fixture
def json_store():
    temp_dir = tempfile.mkdtemp()
    # Explicitly use TestArtifact
    store = FsJsonRepo(Path(temp_dir), artifact_type=ArtifactTest)
    yield store
    shutil.rmtree(temp_dir)


@pytest.fixture
def sqlite_repo(tmp_path):
    db_file = tmp_path / "test_tadween.db"
    # Explicitly use TestArtifact
    return SqliteRepo(db_file, artifact_type=ArtifactTest)


@pytest.fixture
def full_artifact():
    """Create a sample artifact for testing."""
    return ArtifactTest(
        id="test-artifact-001",
        metadata=ArtifactTestMetadata(
            checksum="abc123", audio_path=Path("/test/audio.wav"), duration=120.0
        ),
        part_a=ArtifactTestPart(content="data-a"),
        part_b=ArtifactTestPart(content="data-b"),
    )
