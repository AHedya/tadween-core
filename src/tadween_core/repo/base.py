import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Generic, Literal, TypeVar

from tadween_core.types.artifact import BaseArtifact
from tadween_core.types.artifact.part import BaseArtifactPart

PartNameT = TypeVar("PartNameT", bound=str)
ART = TypeVar("ART", bound=BaseArtifact)

FilterOperator = Literal["eq", "ne", "gt", "lt", "ge", "le"]
CriteriaValue = Any | tuple[Any, FilterOperator]
CriteriaDict = dict[str, CriteriaValue]


class BaseArtifactRepo(ABC, Generic[ART, PartNameT]):
    """
    Abstract contract for Artifact Persistence.

    Responsibilities:
    - Root fields (RootModel + eager BaseModel fields) are always persisted
    together as one atomic unit — they are never partially saved.
    - BaseArtifactPart fields are persisted individually and loaded on demand. Expected to be `bytes` and saved as BLOBs.
    If your repo is domain-related, consider adding custom logic for files extensions, part fields serialization, ...etc (tight coupling)

    Type Parameters:
        ART: The artifact type
        PartNameT: Literal of parts names
    """

    def __init__(self, artifact_type: type[ART], logger: logging.Logger | None = None):
        """
        Initialize the repository.

        Args:
            artifact_type: The artifact class to use.
            Must implement get_part_map() method.
        """
        self._artifact_type = artifact_type
        self._part_map = self._artifact_type.get_part_map()
        self._eager_map = self._artifact_type.get_eager_map()
        self.logger = logger or logging.getLogger(
            f"tadween.repo.{self.__class__.__name__}"
        )

    def _resolve_part_names(
        self, include: Sequence[PartNameT] | Literal["all"] | None
    ) -> list[str]:
        """Translate an `include` argument into a concrete list of part names."""
        if include is None:
            return []
        if include == "all":
            return list(self._part_map)
        names = list(include)
        unknown = set(names) - self._part_map.keys()
        if unknown:
            raise ValueError(
                f"Unknown part name(s) for {self._artifact_type.__name__}: {unknown}. "
                f"Valid parts: {set(self._part_map)}"
            )
        return names

    def _resolve_batch_part_names(
        self,
        include: Sequence[Sequence[PartNameT]] | Literal["all"] | None,
        batch_len: int,
    ) -> list[list[str]]:
        """Helper for resolving input parts in batch mode."""
        if include == "all":
            return [self._resolve_part_names("all")] * batch_len
        elif include is None:
            return [[] for _ in range(batch_len)]

        for inc in include:
            names = list(inc)
            unknown = set(names) - self._part_map.keys()
            if unknown:
                raise ValueError(
                    f"Unknown part name(s) for {self._artifact_type.__name__}: {unknown}. "
                    f"Valid parts: {set(self._part_map)}"
                )
        return include

    @abstractmethod
    def filter(
        self,
        criteria: CriteriaDict,
        include: Sequence[PartNameT] | Literal["all"] | None = None,
        max_len: int | None = None,
        **options: Any,
    ) -> dict[str, ART]:
        """
        Filter artifacts based on CriteriaDict against RootModel fields.

        Args:
            criteria: Dictionary of field names to expected values or (value, operator) tuples.
            include: Parts to load.
            max_len: Maximum number of artifacts to return.
            options: Additional options for loading.

        Returns:
            Dictionary mapping artifact ID to fully populated artifact instances.
        """
        pass

    @abstractmethod
    def list_parts(
        self,
        criteria: CriteriaDict,
        max_len: int | None = None,
    ) -> dict[str, dict[str, bool]]:
        """
        Returns a dictionary mapping artifact IDs to a dictionary of their part presence.
        Example: {"art-1": {"audio": True, "transcription": False}}
        """
        pass

    @abstractmethod
    def has_parts(
        self,
        artifact_id: str,
        include: Sequence[PartNameT] | Literal["all"] | None = "all",
    ) -> dict[PartNameT, bool] | None:
        """
        Check for the presence of specific parts for a given artifact.

        Args:
            artifact_id: The ID of the artifact to check.
            include: The parts to check for. If None, returns an empty dict.
                    If "all", checks for all parts defined on the artifact type.

        Returns:
            A dictionary mapping the requested part names to a boolean indicating their presence.
            Returns None if the artifact itself does not exist.
        """
        pass

    @abstractmethod
    def save(
        self,
        artifact: ART,
        include: Sequence[PartNameT] | Literal["all"] | None = "all",
    ) -> None:
        """
        Root + eager fields are always written.  Part behaviour is controlled
        by ``include``:

            include=None   - root/eager only; no part is touched.
            include="all"  - root/eager + every non-None part on the object.
            include=[...]  - root/eager + the listed parts (must be valid names).

        Parts that are `None` on the object are always silently skipped,
        leaving whatever is currently in storage untouched.
        To explicitly remove parts, use delete_parts().

        Raises:
            ValueError: if any name in ``include`` is not a valid part.
        """
        pass

    @abstractmethod
    def save_many(
        self,
        artifacts: Sequence[ART],
        include: Sequence[Sequence[PartNameT]] | Literal["all"] | None = "all",
    ) -> None:
        """Batch interface for `save`"""
        pass

    @abstractmethod
    def load(
        self,
        artifact_id: str,
        include: Sequence[PartNameT] | Literal["all"] | None = None,
        **options: Any,
    ) -> ART | None:
        """
        Load an artifact.

        Root + eager fields are always loaded.  Parts are controlled by
        ``include`` (same semantics as ``save``).  Parts absent in storage
        are returned as ``None`` on the artifact object.

        Returns:
            artifact object or None if not found

        Raises:
            ValueError: if any name in ``include`` is not a valid part.
        """
        pass

    @abstractmethod
    def load_raw(
        self,
        artifact_id: str,
        include: Sequence[PartNameT] | Literal["all"] | None = None,
        **options: Any,
    ) -> dict[str, Any] | None:
        pass

    @abstractmethod
    def load_many(
        self,
        artifact_ids: Sequence[str],
        include: Sequence[Sequence[PartNameT]] | Literal["all"] | None = None,
        **options: Any,
    ) -> dict[str, ART | None]:
        """Batch interface for load."""
        pass

    @abstractmethod
    def load_many_raw(
        self,
        artifact_ids: Sequence[str],
        include: Sequence[Sequence[PartNameT]] | Literal["all"] | None = None,
        **options: Any,
    ) -> dict[str, dict[str, Any] | None]:
        pass

    @abstractmethod
    def save_part(
        self, artifact_id: str, part_name: PartNameT, data: BaseArtifactPart
    ) -> None:
        """
        Atomically overwrite a single part.

        Raises:
            KeyError:   if the root artifact does not exist.
            ValueError: if ``part_name`` is not valid for this artifact type.
        """

    @abstractmethod
    def load_part(
        self, artifact_id: str, part_name: PartNameT
    ) -> BaseArtifactPart | None:
        """
        Load a single part; returns ``None`` if it has never been saved.

        Raises:
            KeyError:   if the root artifact does not exist.
            ValueError: if ``part_name`` is not valid for this artifact type.
        """

    @abstractmethod
    def delete_parts(
        self,
        artifact_id: str,
        parts: Sequence[PartNameT] | Literal["all"],
    ) -> None:
        """
        Remove stored parts without affecting the root or other parts.
            parts="all"  - remove every stored part for this artifact.
            parts=[...]  - remove only the listed parts.

        Silently skips parts that were never saved.

        Raises:
            ValueError: if any name in ``parts`` is not valid.
            KeyError: If artifact doesn't exist.
        """

    @abstractmethod
    def delete_artifact(self, artifact_id: str) -> None:
        """
        Remove the root record and all associated parts.
        No-op if the artifact does not exist.
        """

    @abstractmethod
    def exists(self, artifact_id: str) -> bool:
        """Return ``True`` if the root record is present in storage."""

    def _validate_criteria(self, criteria: CriteriaDict) -> None:
        """Validate that criteria keys are valid RootModel fields."""
        root_type = self._artifact_type.model_fields["root"].annotation
        for key in criteria:
            if key not in root_type.model_fields:
                raise ValueError(
                    f"Filter field {key!r} is not a valid RootModel field "
                    f"for {self._artifact_type.__name__}."
                )

    def _match_criteria(
        self, root_data: dict[str, Any], criteria: CriteriaDict
    ) -> bool:
        """Evaluate if a root dictionary matches the given criteria."""
        for field, crit_val in criteria.items():
            if field not in root_data:
                return False
            actual_val = root_data[field]

            if (
                isinstance(crit_val, tuple)
                and len(crit_val) == 2
                and isinstance(crit_val[1], str)
            ):
                expected_val, op = crit_val
            else:
                expected_val, op = crit_val, "eq"

            if op == "eq":
                if not (actual_val == expected_val):
                    return False
            elif op == "ne":
                if not (actual_val != expected_val):
                    return False
            elif op == "gt":
                if not (actual_val > expected_val):
                    return False
            elif op == "lt":
                if not (actual_val < expected_val):
                    return False
            elif op == "ge":
                if not (actual_val >= expected_val):
                    return False
            elif op == "le":
                if not (actual_val <= expected_val):
                    return False
            else:
                raise ValueError(f"Unsupported operator: {op}")
        return True
