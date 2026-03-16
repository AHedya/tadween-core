from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TypeAlias

import pytest

from tadween_core.repo.base import BaseArtifactRepo

from ..shared_types import (
    ArtifactRoot,
    ArtifactTest,
    ArtifactTestMetadata,
    ArtifactTestPart,
    part_names,
)

BaseRepo: TypeAlias = BaseArtifactRepo[ArtifactTest, part_names]


class RepoContract:
    """
    Defines the mandatory behavior for any Repo backend.
    Classes(repo providers) inheriting from this must provide a 'repo' fixture.
    Each repo must implement core behavior (contract) and any backend specific tests under its test suite
    """

    def test_basic(
        self,
        repo: BaseRepo,
        artifact_root: ArtifactRoot,
        artifact_metadata: ArtifactTestMetadata,
    ):
        id = artifact_root.id
        artifact = ArtifactTest(root=artifact_root, metadata=artifact_metadata)

        repo.save(artifact)
        assert repo.exists(id)
        assert not repo.exists("non-existent-id")

        assert isinstance(repo.load(id), ArtifactTest)
        assert isinstance(repo.load_raw(id), dict)
        assert isinstance(repo.load_many([id]).get(id), ArtifactTest)
        assert isinstance(repo.load_many_raw([id]).get(id), dict)

    def test_save_and_load_parts(
        self,
        repo: BaseRepo,
        partial_artifact,
    ):

        art_id = partial_artifact.id
        repo.save(partial_artifact, include="all")
        db_obj = repo.load(art_id)
        assert db_obj.part_a is None
        # note that db_obj is type hinted as None. As the original schema defines part_a as not optional
        # However, we inject optional parts at runtime, not statically.
        assert db_obj.part_b is None

        db_obj = repo.load(art_id, include="all")
        assert isinstance(db_obj.part_a, ArtifactTestPart)
        assert db_obj.part_b is None

        db_obj = repo.load(art_id, include=["part_a", "part_b"])
        assert isinstance(db_obj.part_a, ArtifactTestPart)
        assert db_obj.part_b is None

        # save new part on the whole artifact
        part_b = ArtifactTestPart()
        db_obj.part_b = part_b
        repo.save(db_obj, include="all")

        db_obj = repo.load(art_id, include=["part_a", "part_b"])
        assert isinstance(db_obj.part_a, ArtifactTestPart)
        assert isinstance(db_obj.part_b, ArtifactTestPart)

        # Update a part
        part = repo.load_part(artifact_id=art_id, part_name="part_a")
        old_content = part.content
        repo.save_part(
            art_id, "part_a", ArtifactTestPart(content="new content better than never")
        )
        new_part = repo.load_part(artifact_id=art_id, part_name="part_a")
        assert old_content != new_part.content

        # save incompatible part
        with pytest.raises(TypeError):
            repo.save_part(art_id, "part_a", ArtifactTestMetadata())

    def test_delete(self, repo: BaseRepo, full_artifact: ArtifactTest):
        id = full_artifact.id
        repo.delete_artifact("non-existing")

        with pytest.raises(KeyError):
            repo.delete_parts("non-existing", "all")

        repo.save(full_artifact, "all")
        repo.delete_parts(id, "all")

        db_art = repo.load(id, "all")
        assert db_art.part_a is None
        assert db_art.part_a is None

        assert repo.exists(id)
        repo.delete_artifact(id)
        assert not repo.exists(id)

    def test_load_missing_ids(self, repo: BaseRepo, full_artifact: ArtifactTest):
        repo.save(full_artifact)
        results = repo.load_many([full_artifact.id, "ghost-id"])
        print(results)
        assert isinstance(results[full_artifact.id], ArtifactTest)
        assert results["ghost-id"] is None

    def test_concurrent_writes(self, repo: BaseRepo, full_artifact: ArtifactTest):
        id = full_artifact.id
        repo.save(full_artifact)

        def worker(i):
            # Each worker updates metadata
            full_artifact = repo.load(id)
            full_artifact.metadata = ArtifactTestMetadata(
                checksum=f"worker-{i}", audio_path=Path(f"/{i}")
            )
            repo.save(full_artifact, include=None)
            return True

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(worker, i) for i in range(20)]
            for f in as_completed(futures):
                assert f.result() is True

        final = repo.load(id)
        assert final.metadata.checksum.startswith("worker-")
