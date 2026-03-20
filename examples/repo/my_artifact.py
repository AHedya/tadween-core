"""
Shared artifact types used across repo examples.

These mirror a realistic media-processing domain:
    - AudioArtifact    : the top-level artifact
    - AudioRoot        : identity + filtration fields (always persisted)
    - AudioMetadata    : eager fields — small, always loaded with root
    - Transcript       : lazy part — potentially large, loaded on demand
    - DiarizationResult: lazy part — potentially large, loaded on demand
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from tadween_core.repo import BaseArtifactRepo
from tadween_core.types.artifact import ArtifactPart, BaseArtifact, RootModel


class AudioRoot(RootModel):
    stage: Literal["uploaded", "transcribed", "diarized"] = "uploaded"
    language: str = "en"


class AudioMetadata(BaseModel):
    file_path: Path
    duration_seconds: float
    sample_rate: int = 16_000


class Transcript(ArtifactPart):
    text: str = ""
    word_count: int = 0


class DiarizationResult(ArtifactPart):
    speaker_count: int = 0
    segments: list[dict] = []


class AudioArtifact(BaseArtifact):
    root: AudioRoot
    metadata: AudioMetadata

    transcript: Transcript | None = None
    diarization: DiarizationResult | None = None


part_names = Literal["transcript", "diarization"]


def run_example(repo: BaseArtifactRepo):
    artifact = AudioArtifact(
        root=AudioRoot(stage="uploaded", language="ar"),
        metadata=AudioMetadata(
            file_path=Path("/data/audio/interview.wav"),
            duration_seconds=342.5,
        ),
        transcript=Transcript(text="hello world", word_count=2),
    )
    aid = artifact.id
    # Save root + eager fields only (no parts written yet)
    repo.save(artifact, include=None)
    assert repo.exists(aid)

    loaded = repo.load(aid)
    assert loaded.transcript is None  # part was not saved
    assert loaded.metadata.duration_seconds == 342.5  # eager field always present

    # Save + load lazy parts
    # persists transcript (diarization is None, skipped not overwritten)
    repo.save(artifact, include="all")

    loaded = repo.load(aid)
    assert loaded.transcript is None  # parts are NOT loaded by default

    loaded = repo.load(aid, include=["transcript"])
    assert loaded.transcript.word_count == 2  # explicitly requested → present

    loaded = repo.load(aid, include="all")
    assert loaded.transcript is not None
    assert loaded.diarization is None  # was never saved → comes back as None

    # save a new part without touching the root
    diarization = DiarizationResult(
        speaker_count=2,
        segments=[{"speaker": "A", "start": 0.0, "end": 5.3}],
    )
    repo.save_part(aid, "diarization", diarization)

    loaded = repo.load(aid, include="all")
    assert loaded.diarization.speaker_count == 2

    # Update a single part in isolation
    old = repo.load_part(aid, "transcript")
    repo.save_part(aid, "transcript", Transcript(text="New world", word_count=3))

    updated = repo.load_part(aid, "transcript")
    assert updated.text != old.text

    # Delete parts without removing the artifact
    repo.delete_parts(aid, parts="all")

    loaded = repo.load(aid, include="all")
    assert loaded.transcript is None
    assert loaded.diarization is None
    assert repo.exists(aid)  # root is still there

    #  Delete the artifact entirely
    repo.delete_artifact(aid)
    assert not repo.exists(aid)
    assert repo.load(aid) is None

    print("All assertions passed.")
