from pathlib import Path

from _types import (
    AudioArtifact,
    AudioMetadata,
    AudioRoot,
    DiarizationResult,
    Transcript,
    part_names,
)

from tadween_core.repo.json import FsJsonRepo

ROOT = Path(__file__).parent
(ROOT / "temp").mkdir(exist_ok=True)

repo = FsJsonRepo[AudioArtifact, part_names](
    base_path=ROOT / "temp" / "json",
    artifact_type=AudioArtifact,
)


artifact = AudioArtifact(
    root=AudioRoot(stage="uploaded", language="ar"),
    metadata=AudioMetadata(
        file_path=Path("/data/audio/interview.wav"),
        duration_seconds=342.5,
    ),
    transcript=Transcript(text="Hello world", word_count=2),
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
