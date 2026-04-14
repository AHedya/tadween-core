import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import BaseModel

from tadween_core.cache.simple_cache import SimpleCache
from tadween_core.handler.base import BaseHandler
from tadween_core.repo.fs import FsRepo
from tadween_core.stage.policy import InterceptionContext, StagePolicyBuilder
from tadween_core.types.artifact import ArtifactPart, BaseArtifact, RootModel
from tadween_core.workflow.workflow import Workflow


class E2ERoot(RootModel):
    stage: str  # metadata


class TextPart(ArtifactPart):
    content: str


class E2EArtifact(BaseArtifact):
    root: E2ERoot
    part_1: TextPart | None = None
    part_2: TextPart | None = None
    part_3: TextPart | None = None


part_names = Literal["part_1", "part_2", "part_3"]


class IngestionInput(BaseModel):
    user_id: str
    text: str


class IngestionOutput(BaseModel):
    artifact_id: str


class ProcessingInput(BaseModel):
    artifact_id: str


class ProcessingOutput(BaseModel):
    artifact_id: str
    processed_text: str


class FinalizationInput(BaseModel):
    artifact_id: str
    processed_text: str


class FinalizationOutput(BaseModel):
    artifact_id: str
    status: str


class BucketSchema(BaseModel):
    cached_text: str | None = None


class IngestionHandler(BaseHandler[IngestionInput, IngestionOutput]):
    def run(self, inputs: IngestionInput) -> IngestionOutput:
        return IngestionOutput(artifact_id=f"art-{inputs.user_id}")


class ProcessingHandler(BaseHandler[ProcessingInput, ProcessingOutput]):
    def run(self, inputs: ProcessingInput) -> ProcessingOutput:
        if inputs.artifact_id == "art-error":
            raise ValueError("Simulated error in processing")
        # Heavy computation simulation
        time.sleep(0.1)
        return ProcessingOutput(
            artifact_id=inputs.artifact_id, processed_text="PROCESSED"
        )


class FinalizationHandler(BaseHandler[FinalizationInput, FinalizationOutput]):
    def run(self, inputs: FinalizationInput) -> FinalizationOutput:
        return FinalizationOutput(artifact_id=inputs.artifact_id, status="DONE")


@pytest.fixture
def e2e_cache():
    c = SimpleCache(BucketSchema)
    yield c
    c.clear()


@pytest.fixture
def e2e_repo():
    temp_dir = tempfile.mkdtemp()
    store = FsRepo[E2EArtifact, part_names](Path(temp_dir), artifact_type=E2EArtifact)
    yield store
    shutil.rmtree(temp_dir)


def test_full_e2e_workflow(
    inmemory_broker, e2e_repo, thread_queue, process_queue, e2e_cache
):
    events = []

    # 1. Ingestion Policy
    def ingestion_success(task_id, msg, result: IngestionOutput, broker, repo, cache):  # noqa: ARG001
        art = E2EArtifact(
            root=E2ERoot(id=result.artifact_id, stage="ingestion"),
            part_1=TextPart(content="raw text"),
        )
        repo.save(art, include="all")
        events.append(f"Ingestion done: {result.artifact_id}")

    ingestion_policy = StagePolicyBuilder[
        IngestionInput, IngestionOutput, Any, Any, Any
    ]().with_on_success(ingestion_success)

    # 2. Processing Policy
    def process_resolve(msg, repo, cache) -> dict:  # noqa: ARG001
        # The payload is the original IngestionInput, so we form the artifact_id from user_id
        return {"artifact_id": f"art-{msg.payload.get('user_id')}"}

    def process_intercept(msg, broker, repo, cache):  # noqa: ARG001
        art_id = f"art-{msg.payload.get('user_id')}"
        bucket = cache.get_bucket(art_id)
        if bucket and bucket.cached_text:
            events.append("Cache hit")
            return InterceptionContext(
                intercepted=True,
                payload=ProcessingOutput(
                    artifact_id=art_id, processed_text=bucket.cached_text
                ),
                reason="cache_hit",
            )
        return InterceptionContext(intercepted=False)

    def process_success(task_id, msg, result: ProcessingOutput, broker, repo, cache):  # noqa: ARG001
        # Update cache
        cache.set_bucket(
            result.artifact_id, BucketSchema(cached_text=result.processed_text)
        )

        # Update repo
        art = repo.load(result.artifact_id, include="all")
        art.part_2 = TextPart(content=result.processed_text)
        art.root.stage = "processing"
        repo.save(art, include="all")
        events.append(f"Processing done: {result.artifact_id}")

    def process_error(msg, error, broker):  # noqa: ARG001
        events.append(f"Processing error: {error}")

    processing_policy = (
        StagePolicyBuilder[ProcessingInput, ProcessingOutput, Any, Any, Any]()
        .with_resolve_inputs(process_resolve)
        .with_intercept(process_intercept)
        .with_on_success(process_success)
        .with_on_error(process_error)
    )

    # 3. Finalization Policy
    def final_resolve(msg, repo, cache) -> dict:  # noqa: ARG001
        return {
            "artifact_id": f"art-{msg.payload.get('user_id')}",
            "processed_text": "from_repo",
        }

    def final_success(task_id, msg, result: FinalizationOutput, broker, repo, cache):  # noqa: ARG001
        art = repo.load(result.artifact_id, include="all")
        art.part_3 = TextPart(content=result.status)
        art.root.stage = "final"
        repo.save(art, include="all")
        events.append(f"Finalization done: {result.artifact_id}")

    finalization_policy = (
        StagePolicyBuilder[FinalizationInput, FinalizationOutput, Any, Any, Any]()
        .with_resolve_inputs(final_resolve)
        .with_on_success(final_success)
    )

    # Build Workflow
    wf = Workflow(
        broker=inmemory_broker,
        repo=e2e_repo,
        cache=e2e_cache,
        # None for propagating message payload to streamline stages messages.
        default_payload_extractor=lambda x: None,
    )
    wf.add_stage(
        "ingestion",
        IngestionHandler(),
        policy=ingestion_policy,
        task_queue=thread_queue,
    )
    wf.add_stage(
        "processing",
        ProcessingHandler(),
        policy=processing_policy,
        task_queue=process_queue,
    )
    wf.add_stage(
        "finalization",
        FinalizationHandler(),
        policy=finalization_policy,
        task_queue=thread_queue,
    )

    wf.link("ingestion", "processing").link("processing", "finalization")
    wf.set_entry_point("ingestion")
    wf.build()

    # Helper to wait for the entire workflow to settle
    def wait_for_settle():
        wf._stages["ingestion"].wait_all(timeout=5)
        wf._stages["processing"].wait_all(timeout=5)
        wf._stages["finalization"].wait_all(timeout=5)
        inmemory_broker.join(timeout=5)

    # Scenario 1: Happy Path
    events.clear()
    wf.submit(IngestionInput(user_id="user1", text="hello"))
    wait_for_settle()
    print("++", events)

    assert "Ingestion done: art-user1" in events
    assert "Processing done: art-user1" in events
    assert "Finalization done: art-user1" in events

    art = e2e_repo.load("art-user1", include="all")
    assert art is not None
    assert art.root.stage == "final"
    assert art.part_1.content == "raw text"
    assert art.part_2.content == "PROCESSED"
    assert art.part_3.content == "DONE"

    # Scenario 2: Cache Hit (Interception)
    events.clear()
    # We submit the same payload to ingestion.
    # Processing should be intercepted due to cache hit.
    wf.submit(IngestionInput(user_id="user1", text="hello2"))
    wait_for_settle()

    assert "Ingestion done: art-user1" in events
    assert "Cache hit" in events
    # Because of interception, processing_success is STILL called for the router to run the inner policy, so "Processing done: art-user1" will be there.
    assert "Processing done: art-user1" in events
    assert "Finalization done: art-user1" in events

    # Scenario 3: Error Handling
    events.clear()
    wf.submit(IngestionInput(user_id="error", text="bad"))
    wait_for_settle()

    assert "Ingestion done: art-error" in events
    assert any("Processing error" in e for e in events)
    assert "Finalization done: art-error" not in events
