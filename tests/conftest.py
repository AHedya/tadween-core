import shutil
import tempfile
from pathlib import Path

import pytest

from tadween_core.repo import SqliteRepo
from tadween_core.repo.json import FsJsonRepo

from ._types import ArtifactTest, ArtifactTestMetadata, ArtifactTestPart


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
