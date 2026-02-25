import fcntl
import json
import logging
import shutil
import time
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import pydantic
from pydantic import BaseModel
from typing_extensions import TypeVar

from tadween_core.types.artifact.base import BaseArtifact
from tadween_core.types.artifact.tadween import (
    ArtifactStage,
    PartName,
    TadweenArtifact,
)

from .base import BaseArtifactRepo

logger = logging.getLogger(__name__)

ART = TypeVar("ART", bound=BaseArtifact, default=TadweenArtifact)
PartNameT = TypeVar("PartNameT", bound=str, default=str)


class LockMode(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class FsJsonRepo(BaseArtifactRepo[ART, PartNameT]):
    """
    Generic filesystem JSON repository.

    Stores each artifact in its own directory under base_path:
        <base_path>/<artifact_id>/
            root.json          # identity, status, scalar fields
            <part_name>.json   # one file per lazy part
            .lock              # advisory fcntl lock file

    Type Parameters:
        ART:       Any BaseArtifact subclass.  Defaults to TadweenArtifact.
        PartNameT: String literal union of valid part names for ART.
    """

    def __init__(
        self,
        base_path: Path,
        artifact_type: type[ART] | None = None,
    ) -> None:
        super().__init__(artifact_type)
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def create(self, artifact: ART) -> None:
        """
        Initialise a new artifact directory and write root.json.
        Raises FileExistsError if the artifact ID already exists.
        Parts are intentionally ignored here; use save_part separately.
        """
        d = self._get_dir(artifact.id)
        if d.exists():
            raise FileExistsError(f"Artifact {artifact.id} already exists")
        d.mkdir(parents=True)
        self._write_root(d, artifact)

    def save(
        self,
        artifact: ART,
        include: Iterable[PartNameT] | Literal["all"] | None = "all",
    ) -> None:
        """
        Persist root fields and (optionally) parts in one call.

        include=None  → root only
        include="all" → root + every non-None part
        include=[...] → root + the named parts
        """
        parts_to_save = self._resolve_parts(include)
        artifact_dir = self._get_dir(artifact.id)
        artifact_dir.mkdir(parents=True, exist_ok=True)

        with self._lock_artifact(artifact.id, LockMode.EXCLUSIVE):
            artifact.updated_at = time.perf_counter()
            self._write_root(artifact_dir, artifact)

            for part_name in parts_to_save:
                data = getattr(artifact, part_name, None)
                if data is not None:
                    self.save_part(artifact.id, part_name, data)

    def load(
        self,
        artifact_id: str,
        include: Iterable[PartNameT] | Literal["all"] | None = None,
    ) -> ART:
        """
        Reconstruct an artifact from disk.

        include=None  → root fields only (parts remain None)
        include="all" → root + every part file that exists on disk
        include=[...] → root + the named parts
        """
        parts_to_load = self._resolve_parts(include)

        with self._lock_artifact(artifact_id, LockMode.SHARED):
            artifact = self.load_root(artifact_id)

            for part_name in parts_to_load:
                data = self.load_part(artifact_id, part_name)
                if data is not None:
                    setattr(artifact, part_name, data)

            return artifact

    def delete(self, artifact_id: str) -> None:
        """
        Remove the entire artifact directory.

        WARNING: Not safe for concurrent deletion — callers must coordinate
        externally if multiple processes may delete the same artifact.
        """
        artifact_dir = self._get_dir(artifact_id)
        if artifact_dir.exists():
            with self._lock_artifact(artifact_id, LockMode.EXCLUSIVE):
                shutil.rmtree(artifact_dir)

    def exists(self, artifact_id: str) -> bool:
        return (self._get_dir(artifact_id) / "root.json").exists()

    def save_part(
        self,
        artifact_id: str,
        part_name: PartNameT,
        data: BaseModel,
        validate: bool = False,
    ) -> None:
        """
        Atomically write a single part file.
        """
        if part_name not in self._part_types:
            raise ValueError(f"Unknown part: {part_name}")

        d = self._get_dir(artifact_id)
        if not d.exists():
            raise FileNotFoundError(f"Artifact {artifact_id} root does not exist")

        if validate:
            try:
                self._part_types[part_name].model_validate(data)
            except pydantic.ValidationError:
                logger.error(
                    f"Validation error. Failed saving artifact part:"
                    f"[{part_name}] for Artifact with id: {artifact_id}"
                    f"data to save is of type: {type(data)}"
                )

        self._write_atomic(self._get_part_path(artifact_id, part_name), data)

    def load_part(self, artifact_id: str, part_name: PartNameT) -> BaseModel | None:
        """
        Deserialise a single part from its JSON file.
        Returns None if the file does not exist yet.
        """
        if part_name not in self._part_types:
            raise ValueError(f"Unknown part: {part_name!r}")

        target = self._get_part_path(artifact_id, part_name)
        if not target.exists():
            return None

        model_cls = self._part_types[part_name]
        return model_cls.model_validate_json(target.read_text(encoding="utf-8"))

    def load_root(self, artifact_id: str) -> ART:
        p = self._get_part_path(artifact_id, "root")
        if not p.exists():
            raise FileNotFoundError(f"Artifact {artifact_id} not found")
        return self._artifact_type.model_validate_json(p.read_text(encoding="utf-8"))

    def _get_dir(self, artifact_id: str) -> Path:
        return self.base_path / artifact_id

    def _get_part_path(self, artifact_id: str, name: str) -> Path:
        return self._get_dir(artifact_id) / f"{name}.json"

    def _write_root(self, artifact_dir: Path, artifact: ART) -> None:
        """Serialize root fields, excluding lazy parts."""
        exclude = set(self._part_types.keys())
        self._write_atomic(
            artifact_dir / "root.json", artifact.model_dump(exclude=exclude)
        )

    def _resolve_parts(
        self,
        include: Iterable[PartNameT] | Literal["all"] | None,
    ) -> set[PartNameT]:
        if include == "all":
            return set(self._part_types.keys())
        if include is None:
            return set()
        return set(include)

    def _write_atomic(
        self,
        target_path: Path,
        data: dict[str, Any] | BaseModel,
    ) -> None:
        """
        Write via a sibling .tmp file then rename.
        The rename is atomic on POSIX filesystems, so readers never see
        a partially-written file.
        """
        content = (
            data.model_dump_json()
            if isinstance(data, BaseModel)
            else json.dumps(data, default=str)
        )
        tmp_path = target_path.with_suffix(".tmp")
        try:
            tmp_path.write_text(content, encoding="utf-8")
            tmp_path.replace(target_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            logger.exception("Failed writing %s", target_path)
            raise

    @contextmanager
    def _lock_artifact(
        self,
        artifact_id: str,
        mode: LockMode,
    ) -> Generator[Path, None, None]:
        """
        Advisory fcntl lock scoped to a single artifact directory.

        SHARED   → multiple concurrent readers allowed.
        EXCLUSIVE → blocks until all other locks are released.
        """
        artifact_dir = self._get_dir(artifact_id)
        if not artifact_dir.exists():
            raise FileNotFoundError(f"Artifact not found: {artifact_id}")

        lock_file = artifact_dir / ".lock"
        lock_fd = None
        try:
            lock_fd = open(lock_file, "a")
            flag = fcntl.LOCK_SH if mode == LockMode.SHARED else fcntl.LOCK_EX
            fcntl.flock(lock_fd.fileno(), flag)
            yield artifact_dir
        finally:
            if lock_fd is not None:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
                lock_fd.close()


class TadweenFsJsonRepo(FsJsonRepo[TadweenArtifact, PartName]):
    """
    Ready-to-use filesystem JSON repo for TadweenArtifact.
    Locks in TadweenArtifact as the artifact type so callers never need
    to supply type parameters or artifact_type= themselves:
        repo = TadweenFsJsonRepo(base_path=Path("/data/artifacts"))
    """

    def __init__(self, base_path: Path) -> None:
        super().__init__(base_path, artifact_type=TadweenArtifact)

    def update_status(
        self,
        artifact_id: str,
        stage: ArtifactStage,
        error_stage: ArtifactStage | None = None,
    ) -> None:
        """
        Lightweight status update — reads, patches, and re-writes root.json
        without loading or touching any parts.

        Kept here (not on the generic base) because ArtifactStage is
        Tadween-specific; the base repo knows nothing about stage fields.
        """
        root_path = self._get_part_path(artifact_id, "root")

        with self._lock_artifact(artifact_id, LockMode.EXCLUSIVE):
            if not root_path.exists():
                raise FileNotFoundError(f"Artifact {artifact_id} not found")

            data = json.loads(root_path.read_text(encoding="utf-8"))
            data["current_stage"] = stage
            if error_stage is not None:
                data["error_stage"] = error_stage
            data["updated_at"] = time.perf_counter()
            self._write_atomic(root_path, data)
