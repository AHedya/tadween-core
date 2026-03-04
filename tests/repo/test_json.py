from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa
from pathlib import Path  # noqa
from typing import TypeAlias

import pytest  # noqa

from tadween_core.repo.json import FsJsonRepo

from .._types import ArtifactTest, ArtifactTestMetadata, ArtifactTestPart, part_names  # noqa

FsJsonTestRepo: TypeAlias = FsJsonRepo[ArtifactTest, part_names]


def test_json_basic(
    json_repo: FsJsonTestRepo,
    artifact_root,
    artifact_metadata,
):
    id = artifact_root.id
    artifact = ArtifactTest(
        root=artifact_root,
        metadata=artifact_metadata,
    )
    json_repo.save(artifact)
    assert json_repo.exists(id)
    assert not json_repo.exists("sql-none")

    assert isinstance(json_repo.load(id), ArtifactTest)
    assert isinstance(json_repo.load_raw(id), dict)
    assert isinstance(json_repo.load_many([id]).get(id), ArtifactTest)
    assert isinstance(json_repo.load_many_raw([id]).get(id), dict)


def test_json_save_and_load_parts(
    json_repo: FsJsonTestRepo,
    partial_artifact: ArtifactTest,
):
    art_id = partial_artifact.id

    json_repo.save(partial_artifact, include="all")
    db_obj = json_repo.load(art_id)
    assert db_obj.part_a is None
    # note that db_obj is type hinted as None. As the original schema defines part_a as not optional
    # However, we inject optional parts at runtime, not statically.
    assert db_obj.part_b is None

    db_obj = json_repo.load(art_id, include="all")
    assert isinstance(db_obj.part_a, ArtifactTestPart)
    assert db_obj.part_b is None

    db_obj = json_repo.load(art_id, include=["part_a", "part_b"])
    assert isinstance(db_obj.part_a, ArtifactTestPart)
    assert db_obj.part_b is None

    # save new part on the whole artifact
    part_b = ArtifactTestPart()
    db_obj.part_b = part_b
    json_repo.save(db_obj)

    db_obj = json_repo.load(art_id, include=["part_a", "part_b"])
    assert isinstance(db_obj.part_a, ArtifactTestPart)
    assert isinstance(db_obj.part_b, ArtifactTestPart)

    # Update a part
    part = json_repo.load_part(artifact_id=art_id, part_name="part_a")
    old_content = part.content
    json_repo.save_part(
        art_id, "part_a", ArtifactTestPart(content="new content better than never")
    )
    new_part = json_repo.load_part(artifact_id=art_id, part_name="part_a")
    assert old_content != new_part.content

    # save incompatible part

    with pytest.raises(TypeError):
        json_repo.save_part(art_id, "part_a", ArtifactTestMetadata())


def test_json_delete(
    json_repo: FsJsonTestRepo,
    full_artifact: ArtifactTest,
):
    id = full_artifact.id
    json_repo.delete_artifact("non-existing")

    with pytest.raises(KeyError):
        json_repo.delete_parts("non-existing", "all")

    json_repo.save(full_artifact, "all")
    json_repo.delete_parts(id, "all")

    db_art = json_repo.load(id, "all")
    assert db_art.part_a is None
    assert db_art.part_a is None

    assert json_repo.exists(id)
    json_repo.delete_artifact(id)
    assert not json_repo.exists(id)


def test_json_concurrent_writes(
    json_repo: FsJsonTestRepo,
    full_artifact: ArtifactTest,
):
    """Test thread-safety of json implementation."""
    id = full_artifact.id
    json_repo.save(full_artifact)

    def worker(i):
        # Each worker updates metadata
        full_artifact = json_repo.load(id)
        full_artifact.metadata = ArtifactTestMetadata(
            checksum=f"worker-{i}", audio_path=Path(f"/{i}")
        )
        json_repo.save(full_artifact, include=None)
        return True

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker, i) for i in range(20)]
        for f in as_completed(futures):
            assert f.result() is True

    final = json_repo.load(id)
    assert final.metadata.checksum.startswith("worker-")
