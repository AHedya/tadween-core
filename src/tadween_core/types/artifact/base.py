import time
from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict, Field


class ArtifactPart(BaseModel):
    """Base class for all heavy artifact parts.
    Inherit from this class to mark a field as lazy-loaded part.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)


class BaseArtifact(ABC, BaseModel):
    """Contract for any artifact object.

    Any artifact class must implement get_part_map and part_names methods.
    """

    id: str
    created_at: float = Field(default_factory=lambda: time.perf_counter())
    updated_at: float = Field(default_factory=lambda: time.perf_counter())

    metadata: dict | BaseModel | None = None

    @classmethod
    @abstractmethod
    def get_part_map(cls) -> dict[str, type[ArtifactPart]]:
        """Returns mapping of part name to part type."""
        ...

    @classmethod
    @abstractmethod
    def part_names(cls) -> frozenset[str]:
        """Returns set of part names for this artifact."""
        ...
