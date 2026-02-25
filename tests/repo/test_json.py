import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Process, Queue
from pathlib import Path

import pytest

from tadween_core.repo.json import FsJsonRepo, LockMode

from ..conftest import ArtifactTest, ArtifactTestMetadata, ArtifactTestPart


def test_create_and_exists(json_store: FsJsonRepo):
    artifact = ArtifactTest(id="test-1")
    json_store.create(artifact)
    assert json_store.exists("test-1")
    assert not json_store.exists("non-existent")


def test_create_already_exists(json_store: FsJsonRepo):
    artifact = ArtifactTest(id="test-1")
    json_store.create(artifact)
    with pytest.raises(FileExistsError):
        json_store.create(artifact)


def test_save_and_load_root(json_store: FsJsonRepo):
    artifact = ArtifactTest(
        id="test-2",
        metadata=ArtifactTestMetadata(
            checksum="abc", audio_path=Path("/tmp/audio.wav")
        ),
    )
    json_store.save(artifact, include=None)  # Only root

    loaded = json_store.load("test-2")
    assert loaded.id == "test-2"
    assert loaded.metadata.checksum == "abc"
    assert loaded.part_a is None


def test_save_and_load_parts(json_store: FsJsonRepo):
    artifact = ArtifactTest(id="test-3")
    artifact.part_a = ArtifactTestPart(content="hello")

    json_store.save(artifact, include=["part_a"])

    # Load only root
    loaded_root = json_store.load("test-3", include=None)
    assert loaded_root.part_a is None

    # Load with part_a
    loaded_part_a = json_store.load("test-3", include=["part_a"])
    assert loaded_part_a.part_a.content == "hello"


def test_save_part_directly(json_store: FsJsonRepo):
    artifact = ArtifactTest(id="test-4")
    json_store.create(artifact)

    part_data = ArtifactTestPart(content="direct save")
    json_store.save_part("test-4", "part_a", part_data)

    loaded = json_store.load("test-4", include=["part_a"])
    assert loaded.part_a.content == "direct save"


def test_delete(json_store: FsJsonRepo):
    artifact = ArtifactTest(id="test-5")
    json_store.create(artifact)
    assert json_store.exists("test-5")

    json_store.delete("test-5")
    assert not json_store.exists("test-5")


# --- Concurrency Tests ---


def test_concurrent_reads_no_blocking(
    json_store: FsJsonRepo, full_artifact: ArtifactTest
):
    """
    Multiple processes reading the same artifact concurrently.
    With SHARED locks, they should NOT block each other.
    """
    # Setup: Save an artifact
    json_store.save(full_artifact)
    artifact_id = full_artifact.id

    # Test: 10 concurrent readers
    num_readers = 10

    with ProcessPoolExecutor(max_workers=num_readers) as executor:
        start_time = time.perf_counter()

        futures = [
            executor.submit(_worker_read_artifact, json_store.base_path, artifact_id, i)
            for i in range(num_readers)
        ]

        results = [f.result() for f in as_completed(futures)]
        elapsed = time.perf_counter() - start_time

    # Verify: All reads succeeded
    assert all(r["success"] for r in results), f"Some reads failed: {results}"

    # Performance check: With shared locks, concurrent reads should be fast
    print(f"10 concurrent reads took {elapsed:.3f}s")
    assert elapsed < 2.0, (
        f"Concurrent reads took too long ({elapsed}s) - might be blocking"
    )


def test_write_blocks_reads(json_store: FsJsonRepo, full_artifact: ArtifactTest):
    """
    A writer should block readers until write completes.
    """
    json_store.save(full_artifact)
    artifact_id = full_artifact.id

    results_queue = Queue()

    def slow_writer():
        """Writer that holds lock for a while."""
        store = FsJsonRepo(json_store.base_path, artifact_type=ArtifactTest)

        # Manually acquire lock to simulate slow write
        with store._lock_artifact(artifact_id, mode=LockMode.EXCLUSIVE):
            results_queue.put(
                {"event": "writer_acquired_lock", "time": time.perf_counter()}
            )
            time.sleep(0.5)

            # Do the actual write
            artifact = ArtifactTest(
                id=artifact_id,
                metadata=full_artifact.metadata,
            )
            store._write_atomic(
                store._get_dir(artifact_id) / "root.json",
                artifact,
            )
            results_queue.put(
                {"event": "writer_released_lock", "time": time.perf_counter()}
            )

    def reader():
        """Reader that should be blocked by writer."""
        store = FsJsonRepo(json_store.base_path, artifact_type=ArtifactTest)
        results_queue.put(
            {"event": "reader_attempting_lock", "time": time.perf_counter()}
        )

        artifact = store.load(
            artifact_id,
        )
        results_queue.put(
            {"event": "reader_acquired_lock", "time": time.perf_counter()}
        )
        return artifact

    # Start writer first
    writer_process = Process(target=slow_writer)
    writer_process.start()

    # Give writer time to acquire lock
    time.sleep(0.1)

    # Start reader (should block)
    reader_process = Process(target=reader)
    reader_process.start()

    # Wait for both to complete
    writer_process.join(timeout=2)
    reader_process.join(timeout=2)

    # Collect results
    events = []
    while not results_queue.empty():
        events.append(results_queue.get())

    events.sort(key=lambda x: x["time"])

    # Verify: Writer acquired -> Writer released -> Reader acquired
    event_types = [e["event"] for e in events]
    assert event_types == [
        "writer_acquired_lock",
        "reader_attempting_lock",
        "writer_released_lock",
        "reader_acquired_lock",
    ], f"Wrong event order: {event_types}"

    # Verify: Reader was blocked for ~500ms
    writer_acquired = next(
        e["time"] for e in events if e["event"] == "writer_acquired_lock"
    )
    reader_acquired = next(
        e["time"] for e in events if e["event"] == "reader_acquired_lock"
    )
    blocked_time = reader_acquired - writer_acquired

    assert blocked_time >= 0.4, f"Reader wasn't properly blocked ({blocked_time:.3f}s)"


def test_concurrent_writes_serialize(json_store: FsJsonRepo):
    """
    Multiple processes writing to the same artifact.
    Writes should serialize (only one at a time).
    Final state should be consistent (from one of the writers).
    """
    artifact_id = "test-concurrent-writes"
    num_writers = 5

    with ProcessPoolExecutor(max_workers=num_writers) as executor:
        futures = [
            executor.submit(_worker_save_artifact, json_store.base_path, artifact_id, i)
            for i in range(num_writers)
        ]

        results = [f.result() for f in as_completed(futures)]

    # Verify: All writes succeeded (no crashes)
    assert all(r["success"] for r in results), f"Some writes failed: {results}"

    # Verify: Final artifact is consistent
    final_artifact = json_store.load(artifact_id, include="all")
    assert final_artifact.id == artifact_id
    assert final_artifact.metadata is not None
    assert final_artifact.metadata.checksum.startswith("worker-")

    # Extract worker ID from checksum
    worker_id = int(final_artifact.metadata.checksum.split("-")[1])
    assert final_artifact.metadata.duration == float(worker_id)


def test_save_race_with_delete(json_store: FsJsonRepo):
    """
    Test race between save (creating) and delete operations.
    Should not leave partial artifacts or crash.
    """
    artifact_id = "race-test-artifact"

    def creator():
        """Repeatedly create artifact."""
        store = FsJsonRepo(json_store.base_path, artifact_type=ArtifactTest)
        for i in range(10):
            artifact = ArtifactTest(
                id=artifact_id,
                metadata=ArtifactTestMetadata(
                    checksum=f"iter-{i}", audio_path=Path(f"/test/{i}.wav")
                ),
            )
            try:
                store.save(artifact)
            except (FileNotFoundError, FileExistsError):
                # Delete happened or create failed, that's OK
                pass
            time.sleep(0.01)

    def deleter():
        """Repeatedly delete artifact."""
        store = FsJsonRepo(json_store.base_path, artifact_type=ArtifactTest)
        time.sleep(0.005)  # Slight offset

        for _ in range(10):
            try:
                if store.exists(artifact_id):
                    store.delete(artifact_id)
            except FileNotFoundError:
                # Already deleted, that's OK
                pass
            time.sleep(0.01)

    creator_process = Process(target=creator)
    deleter_process = Process(target=deleter)

    creator_process.start()
    deleter_process.start()

    creator_process.join(timeout=3)
    deleter_process.join(timeout=3)

    # Verify: No partial artifacts left
    if json_store.exists(artifact_id):
        artifact = json_store.load(artifact_id)
        assert artifact.id == artifact_id


def test_high_contention_read_write(
    json_store: FsJsonRepo, full_artifact: ArtifactTest
):
    """
    Stress test: Many readers and writers competing for same artifact.
    Should maintain consistency without corruption.
    """
    json_store.save(full_artifact)
    artifact_id = full_artifact.id

    num_workers = 10  # Reduced for faster CI
    iterations = 5

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(
                _worker_read_write_cycle,
                json_store.base_path,
                artifact_id,
                i,
                iterations,
            )
            for i in range(num_workers)
        ]

        results = [f.result() for f in as_completed(futures)]

    # Verify: All operations succeeded
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    if failed:
        for f in failed:
            print(f"  Worker {f['worker_id']}: {f.get('error')}")

    assert len(successful) == num_workers, f"{len(failed)} workers failed"

    # Verify: Final artifact is consistent
    final_artifact = json_store.load(artifact_id, include="all")
    assert final_artifact.id == artifact_id
    assert final_artifact.metadata is not None
    assert final_artifact.metadata.checksum.startswith("worker-")


# --- Helper functions ---


def _worker_save_artifact(store_path: Path, artifact_id: str, worker_id: int) -> dict:
    """Worker function to save an artifact (runs in separate process)."""
    try:
        store = FsJsonRepo(store_path, artifact_type=ArtifactTest)
        artifact = ArtifactTest(
            id=artifact_id,
            metadata=ArtifactTestMetadata(
                checksum=f"worker-{worker_id}",
                audio_path=Path(f"/test/worker-{worker_id}.wav"),
                duration=float(worker_id),
            ),
        )
        store.save(artifact)
        return {"success": True, "worker_id": worker_id}
    except Exception as e:
        return {"success": False, "worker_id": worker_id, "error": str(e)}


def _worker_read_artifact(store_path: Path, artifact_id: str, worker_id: int) -> dict:
    try:
        store = FsJsonRepo(store_path, artifact_type=ArtifactTest)
        artifact = store.load(artifact_id, include="all")
        assert artifact.id == artifact_id
        return {"success": True, "worker_id": worker_id, "artifact_id": artifact.id}
    except Exception as e:
        return {"success": False, "worker_id": worker_id, "error": str(e)}


def _worker_read_write_cycle(
    store_path: Path, artifact_id: str, worker_id: int, iterations: int
) -> dict:
    """Worker that repeatedly reads and writes (runs in separate process)."""
    try:
        store = FsJsonRepo(store_path, artifact_type=ArtifactTest)
        for i in range(iterations):
            # Read
            artifact = store.load(artifact_id)
            # Modify
            artifact.metadata.checksum = f"worker-{worker_id}-iter-{i}"
            # Write back
            store.save(artifact)
            time.sleep(0.001)
        return {"success": True, "worker_id": worker_id, "iterations": iterations}
    except Exception as e:
        return {"success": False, "worker_id": worker_id, "error": str(e)}
