import random
import shutil
import tempfile
from pathlib import Path

import pytest

from tadween_core.repo import SqliteRepo
from tadween_core.repo.json import FsJsonRepo

from .shared_types import (
    ArtifactRoot,
    ArtifactTest,
    ArtifactTestMetadata,
    ArtifactTestPart,
    part_names,
)


@pytest.fixture
def json_repo():
    temp_dir = tempfile.mkdtemp()
    store = FsJsonRepo(Path(temp_dir), artifact_type=ArtifactTest)
    yield store
    shutil.rmtree(temp_dir)


@pytest.fixture
def sqlite_repo(tmp_path) -> SqliteRepo[ArtifactTest, part_names]:
    db_file = tmp_path / "test_sqlite_repo.db"
    return SqliteRepo(db_file, artifact_type=ArtifactTest)


@pytest.fixture
def artifact_metadata() -> ArtifactTestMetadata:
    return ArtifactTestMetadata(
        checksum="random checksum",
        file_path=Path("/home/random_path"),
        duration=random.randint(0, 20) + random.random(),
    )


@pytest.fixture
def artifact_root() -> ArtifactRoot:
    return ArtifactRoot(
        stage=f"stage-{random.randint(1, 5)}",
    )


@pytest.fixture
def partial_artifact() -> ArtifactTest:
    return ArtifactTest(
        root=ArtifactRoot(
            stage=f"stage-{random.randint(1, 5)}",
        ),
        metadata=ArtifactTestMetadata(
            checksum="partial artifact metadata",
            file_path=Path("/home/random_path"),
            duration=random.randint(0, 20) + random.random(),
        ),
        part_a=ArtifactTestPart(),
    )


@pytest.fixture
def full_artifact() -> ArtifactTest:
    return ArtifactTest(
        root=ArtifactRoot(
            stage=f"stage-{random.randint(1, 5)}",
        ),
        metadata=ArtifactTestMetadata(
            checksum="full artifact metadata",
            file_path=Path("/home/random_path"),
            duration=random.randint(0, 20) + random.random(),
        ),
        part_a=ArtifactTestPart(),
        part_b=ArtifactTestPart(),
    )
