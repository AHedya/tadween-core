from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import (
    Generic,
    Literal,
)

from pydantic import BaseModel
from typing_extensions import TypeVar

from tadween_core.types.artifact.tadween import (
    BaseArtifact,
    PartName,
    TadweenArtifact,
)

# Type variables
# PartNameT must come first because ART has a default
PartNameT = TypeVar("PartNameT", bound=str, default=str)
# ART bounds BaseArtifact, defaults to TadweenArtifact
ART = TypeVar("ART", bound=BaseArtifact, default=TadweenArtifact)


class BaseArtifactRepo(ABC, Generic[ART, PartNameT]):
    """
    Abstract contract for Artifact Persistence.
    Repository dealing with artifact parts atomically.

    Type Parameters:
        ART: The artifact type (default: TadweenArtifact)
        PartNameT: The part name type (default: PartName for TadweenArtifact)
    """

    def __init__(self, artifact_type: type[ART] | None = None):
        """
        Initialize the repository.

        Args:
            artifact_type: The artifact class to use. Defaults to TadweenArtifact.
            Must implement get_part_map() method.
        """
        self._artifact_type: type[ART] = artifact_type or TadweenArtifact
        self._part_types = self._artifact_type.get_part_map()

    @abstractmethod
    def create(self, artifact: ART) -> None:
        """
        Initialize a new artifact entry.
        Should fail if ID already exists.
        Only saves Root fields (id, status, metadata), ignores Parts.
        """
        pass

    @abstractmethod
    def save(
        self,
        artifact: ART,
        include: Iterable[PartNameT] | Literal["all"] | None = "all",
    ) -> None:
        """
        Batch save the artifact.
        - include=None: Only saves root fields (status, etc).
        - include="all": Saves root + all non-None parts.
        - include=[...]: Saves root + specific parts.

        Validates that included parts are valid for the artifact type.
        """
        pass

    @abstractmethod
    def load(
        self,
        artifact_id: str,
        include: Iterable[PartNameT] | Literal["all"] | None = None,
    ) -> ART:
        """
        Batch load the artifact.
        - include=None: Loads only root fields (parts will be None).
        - include="all": Loads root + every part found in storage.

        Validates loaded parts match expected schema.
        """
        pass

    @abstractmethod
    def save_part(
        self, artifact_id: str, part_name: PartNameT, data: BaseModel
    ) -> None:
        """
        Atomically save/overwrite a specific part.
        Assumes the root artifact exists.
        """
        pass

    @abstractmethod
    def load_part(self, artifact_id: str, part_name: PartNameT) -> BaseModel | None:
        """
        Load a specific heavy part.
        Returns None if the part does not exist yet.
        """
        pass

    @abstractmethod
    def delete(self, artifact_id: str) -> None:
        pass

    @abstractmethod
    def exists(self, artifact_id: str) -> bool:
        """Check if root artifact record exists."""
        pass


# Default type alias for TadweenArtifact repository
TadweenRepo = BaseArtifactRepo[TadweenArtifact, PartName]
