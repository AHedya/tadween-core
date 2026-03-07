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

from tadween_core.types.artifact import BaseArtifact
from tadween_core.types.artifact.base import ArtifactPart, BaseModel, RootModel


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
