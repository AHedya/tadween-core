import threading
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
    AudioPart,
    PickleCustomPart,
    PickleNumpyPart,
    generate_random_array,
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
        n_workers = 10
        barrier = threading.Barrier(n_workers, timeout=3)

        def worker(i):
            # Each worker updates metadata
            full_artifact = repo.load(id)
            full_artifact.metadata = ArtifactTestMetadata(
                checksum=f"worker-{i}", audio_path=Path(f"/{i}")
            )
            barrier.wait()
            repo.save(full_artifact, include=None)
            return True

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(worker, i) for i in range(20)]
            for f in as_completed(futures):
                assert f.result() is True

        final = repo.load(id)
        assert final.metadata.checksum.startswith("worker-")

    def test_complex_types(self, repo: BaseRepo, full_artifact: ArtifactTest):
        repo.save(full_artifact)
        # audio is a complex type containing numpy.ndarray data.
        # While the default part serialization type is `bytes`, FsJsonRepo requires human readable format,
        # so our model needs to know how to express itself (implement mode='json')
        repo.load_raw(full_artifact.id, include=["audio"])
        art = repo.load(full_artifact.id, include="all")
        assert isinstance(art, ArtifactTest)
        assert isinstance(art.audio, AudioPart)

        array_len = len(art.audio.voice)
        new_audio = AudioPart(voice=generate_random_array(array_len + 2))
        repo.save_part(art.id, "audio", new_audio)

        updated_art = repo.load(full_artifact.id, include="all")
        assert isinstance(updated_art.audio, AudioPart)
        new_array_len = len(updated_art.audio.voice)

        assert new_array_len > array_len

        # None is skipped. Never overwriting artifact part
        updated_art.audio = None
        updated_art = repo.save(updated_art, "all")

        audio_part = repo.load_part(full_artifact.id, "audio")
        assert isinstance(audio_part, AudioPart)

    def test_pickle_part_complex_types(
        self, repo: BaseRepo, full_pickle_artifact: ArtifactTest
    ):
        """Test PicklePart serialization with complex types (no custom validators needed)."""
        art_id = full_pickle_artifact.id
        original_numpy_len = len(full_pickle_artifact.pickle_numpy.data)
        original_custom_value = full_pickle_artifact.pickle_custom.custom_obj.value
        original_custom_name = full_pickle_artifact.pickle_custom.custom_obj.name

        repo.save(full_pickle_artifact, include="all")

        loaded = repo.load(art_id, include="all")
        assert isinstance(loaded.pickle_numpy, PickleNumpyPart)
        assert isinstance(loaded.pickle_custom, PickleCustomPart)

        assert len(loaded.pickle_numpy.data) == original_numpy_len
        assert loaded.pickle_custom.custom_obj.value == original_custom_value
        assert loaded.pickle_custom.custom_obj.name == original_custom_name

        new_numpy = PickleNumpyPart(data=generate_random_array(150))
        repo.save_part(art_id, "pickle_numpy", new_numpy)

        updated = repo.load(art_id, include=["pickle_numpy"])
        assert isinstance(updated.pickle_numpy, PickleNumpyPart)
        assert len(updated.pickle_numpy.data) == 150
        assert updated.pickle_custom is None

    def test_filter(self, repo: BaseRepo, artifact_metadata: ArtifactTestMetadata):
        # Create artifacts for filtering
        art1 = ArtifactTest(
            root=ArtifactRoot(id="filter-1", stage="init", created_at=10.0),
            metadata=artifact_metadata,
        )
        art2 = ArtifactTest(
            root=ArtifactRoot(id="filter-2", stage="init", created_at=20.0),
            metadata=artifact_metadata,
        )
        art3 = ArtifactTest(
            root=ArtifactRoot(id="filter-3", stage="done", created_at=30.0),
            metadata=artifact_metadata,
        )

        repo.save_many([art1, art2, art3])

        # Exact match (implicit 'eq')
        res = repo.filter({"stage": "init"})
        assert set(res.keys()) == {"filter-1", "filter-2"}

        # Tuple match (operator 'gt')
        res = repo.filter({"created_at": (20.0, "gt")})
        assert set(res.keys()) == {"filter-3"}

        # Multiple criteria
        res = repo.filter({"stage": ("init", "eq"), "created_at": (15.0, "lt")})
        assert set(res.keys()) == {"filter-1"}

        # Test max_len parameter
        res_all = repo.filter({})
        assert len(res_all) >= 3
        res_limit = repo.filter({}, max_len=2)
        assert len(res_limit) == 2

        # Test max_len with criteria
        res_init_limit = repo.filter({"stage": "init"}, max_len=1)
        assert len(res_init_limit) == 1
        assert list(res_init_limit.keys())[0] in {"filter-1", "filter-2"}

        # Complex case 1: Empty results
        res_empty = repo.filter({"stage": "nonexistent"})
        assert len(res_empty) == 0

        # Complex case 2: Filtering by 'id' explicitly with 'ne' operator
        res_ne = repo.filter({"id": ("filter-2", "ne"), "stage": "init"})
        assert set(res_ne.keys()) == {"filter-1"}

        # Complex case 3: Using 'include' parameter to load a specific part during filter
        art1.part_a = ArtifactTestPart(content="part_a_data")
        repo.save(art1, include=["part_a"])

        res_include = repo.filter({"id": "filter-1"}, include=["part_a"])
        assert len(res_include) == 1
        loaded_art = res_include["filter-1"]
        assert loaded_art.part_a is not None
        assert loaded_art.part_a.content == "part_a_data"
        assert loaded_art.part_b is None

        # Test validation failure for invalid RootModel field
        with pytest.raises(ValueError, match="is not a valid RootModel field"):
            repo.filter({"unknown_field": "val"})

    def test_list_parts(self, repo: BaseRepo, artifact_metadata: ArtifactTestMetadata):
        art1 = ArtifactTest(
            root=ArtifactRoot(id="list-parts-1", stage="init", created_at=10.0),
            metadata=artifact_metadata,
        )
        art1.part_a = ArtifactTestPart(content="part_a_data")

        art2 = ArtifactTest(
            root=ArtifactRoot(id="list-parts-2", stage="init", created_at=20.0),
            metadata=artifact_metadata,
        )
        art2.part_b = ArtifactTestPart(content="part_b_data")
        art2.audio = AudioPart()

        repo.save_many([art1, art2], include="all")

        # Test basic list_parts
        parts_info = repo.list_parts({"stage": "init"})
        assert "list-parts-1" in parts_info
        assert "list-parts-2" in parts_info

        # Check part presence for art1
        assert parts_info["list-parts-1"]["part_a"] is True
        assert parts_info["list-parts-1"]["part_b"] is False
        assert parts_info["list-parts-1"]["audio"] is False

        # Check part presence for art2
        assert parts_info["list-parts-2"]["part_a"] is False
        assert parts_info["list-parts-2"]["part_b"] is True
        assert parts_info["list-parts-2"]["audio"] is True

        # Test max_len parameter
        parts_info_limit = repo.list_parts({"stage": "init"}, max_len=1)
        assert len(parts_info_limit) == 1
