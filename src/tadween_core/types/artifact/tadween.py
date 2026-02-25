import time
import uuid
from enum import StrEnum, auto
from pathlib import Path
from typing import Literal, get_args

from pydantic import BaseModel, ConfigDict, Field

from .base import ArtifactPart, BaseArtifact


class ArtifactStage(StrEnum):
    CREATED = auto()
    METADATA = auto()
    ASR = auto()
    NORMALIZED = auto()
    LLM = auto()
    ERROR = auto()
    DONE = auto()


class ArtifactMetadata(BaseModel):
    audio_path: Path
    checksum: str | None = None
    duration: float | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ASRResults(ArtifactPart):
    transcription: dict | None = None
    alignment: dict | None = None


class NormalizedContent(ArtifactPart):
    speech_lines: list[dict] = Field(default_factory=list)
    active_speech_duration: float | None = None
    speaker_percentages: dict | None = None


class LLMResult(ArtifactPart):
    analysis: dict | None = None
    reporting: dict | None = None
    final_report: str | None = None


class TadweenArtifact(BaseArtifact):
    """
    The Root Entity holds identity, status, and metadata.
    Heavy parts are Optional and loaded lazily.

    Inherits from BaseArtifact to satisfy the contract.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    current_stage: ArtifactStage = ArtifactStage.CREATED
    error_stage: ArtifactStage | None = None
    # timestamps
    created_at: float = Field(default_factory=lambda: time.perf_counter())
    updated_at: float = Field(default_factory=lambda: time.perf_counter())

    metadata: ArtifactMetadata | None = None

    # Lazy-loaded parts
    asr: ASRResults | None = None
    normalized: NormalizedContent | None = None
    llm: LLMResult | None = None

    @classmethod
    def get_part_map(cls) -> dict[str, type[ArtifactPart]]:
        """
        Introspects the model to find fields that are ArtifactParts.
        Returns: { 'asr': ASRResults, 'llm': LLMResult, ... }
        """
        parts = {}
        for name, field in cls.model_fields.items():
            ann = field.annotation
            # Unwrap Optional[T] -> T
            if hasattr(ann, "__args__"):
                # This handles Optional[ASRResults] which is Union[ASRResults, NoneType]
                candidates = [
                    a
                    for a in ann.__args__
                    if isinstance(a, type) and issubclass(a, ArtifactPart)
                ]
                if candidates:
                    parts[name] = candidates[0]
            elif isinstance(ann, type) and issubclass(ann, ArtifactPart):
                parts[name] = ann
        return parts

    @classmethod
    def part_names(cls) -> frozenset[str]:
        return frozenset(cls.get_part_map().keys())


PartName = Literal["asr", "normalized", "llm"]


def _validate_part_name_sync():
    """Ensure PartName Literal matches actual artifact parts."""
    declared = frozenset(get_args(PartName))
    actual = TadweenArtifact.part_names()

    if declared != actual:
        missing = actual - declared
        extra = declared - actual
        raise TypeError(
            f"PartName Literal out of sync with TadweenArtifact!\n"
            f"  Missing from Literal: {missing}\n"
            f"  Extra in Literal: {extra}\n"
            f"  Update PartName = Literal[...] in artifacts.py"
        )


_validate_part_name_sync()
