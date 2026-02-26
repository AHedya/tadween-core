from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pytest

from tadween_core.repo import SqliteRepo

from .._types import ArtifactTest, ArtifactTestMetadata, ArtifactTestPart


def test_sqlite_create_and_exists(sqlite_repo: SqliteRepo):
    artifact = ArtifactTest(id="sql-1")
    sqlite_repo.create(artifact)
    assert sqlite_repo.exists("sql-1")
    assert not sqlite_repo.exists("sql-none")


def test_sqlite_create_already_exists(sqlite_repo: SqliteRepo):
    artifact = ArtifactTest(id="sql-1")
    sqlite_repo.create(artifact)
    with pytest.raises(FileExistsError):
        sqlite_repo.create(artifact)


def test_sqlite_save_and_load_root(sqlite_repo: SqliteRepo):
    artifact = ArtifactTest(
        id="sql-2",
        metadata=ArtifactTestMetadata(
            checksum="sql-abc", audio_path=Path("/tmp/sql.wav")
        ),
    )
    sqlite_repo.save(artifact, include=None)

    loaded = sqlite_repo.load("sql-2")
    assert loaded.id == "sql-2"
    assert loaded.metadata.checksum == "sql-abc"
    assert loaded.part_a is None


def test_sqlite_save_and_load_parts(sqlite_repo: SqliteRepo):
    artifact = ArtifactTest(id="sql-3")
    artifact.part_a = ArtifactTestPart(content="sqlite hello")

    sqlite_repo.save(artifact, include=["part_a"])

    # Load only root
    loaded_root = sqlite_repo.load("sql-3", include=None)
    assert loaded_root.part_a is None

    # Load with part_a (Verify NO N+1: Should be 2 queries total in implementation)
    loaded_part_a = sqlite_repo.load("sql-3", include=["part_a"])
    assert loaded_part_a.part_a.content == "sqlite hello"


def test_sqlite_save_part_directly(sqlite_repo: SqliteRepo):
    artifact = ArtifactTest(id="sql-4")
    sqlite_repo.create(artifact)

    part_data = ArtifactTestPart(content="direct sql save")
    sqlite_repo.save_part("sql-4", "part_a", part_data)

    loaded = sqlite_repo.load("sql-4", include=["part_a"])
    assert loaded.part_a.content == "direct sql save"


def test_sqlite_load_part_directly(sqlite_repo: SqliteRepo):
    artifact = ArtifactTest(id="sql-load-part")
    artifact.part_a = ArtifactTestPart(content="load me")
    sqlite_repo.save(artifact, include=["part_a"])

    part = sqlite_repo.load_part("sql-load-part", "part_a")
    assert isinstance(part, ArtifactTestPart)
    assert part.content == "load me"


def test_sqlite_delete(sqlite_repo: SqliteRepo):
    artifact = ArtifactTest(id="sql-5")
    sqlite_repo.create(artifact)
    sqlite_repo.save_part("sql-5", "part_a", ArtifactTestPart(content="v"))
    assert sqlite_repo.exists("sql-5")

    sqlite_repo.delete("sql-5")
    assert not sqlite_repo.exists("sql-5")
    # Verify parts are also deleted (Foreign Key cascade)
    assert sqlite_repo.load_part("sql-5", "part_a") is None


def test_sqlite_concurrent_writes(sqlite_repo: SqliteRepo):
    """Test thread-safety of SQLite implementation."""
    artifact_id = "sql-concurrent"
    sqlite_repo.create(ArtifactTest(id=artifact_id))

    def worker(i):
        # Each worker updates metadata
        artifact = sqlite_repo.load(artifact_id)
        artifact.metadata = ArtifactTestMetadata(
            checksum=f"worker-{i}", audio_path=Path(f"/{i}")
        )
        sqlite_repo.save(artifact, include=None)
        return True

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, i) for i in range(10)]
        for f in as_completed(futures):
            assert f.result() is True

    final = sqlite_repo.load(artifact_id)
    assert final.metadata.checksum.startswith("worker-")
