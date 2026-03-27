import fcntl
import json
import shutil
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any

from tadween_core.types.artifact.part import BaseArtifactPart

from .base import ART, BaseArtifactRepo, PartNameT


class LockMode(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class FsRepo(BaseArtifactRepo[ART, PartNameT]):
    """
    Generic filesystem repository.

    Stores each artifact in its own directory under base_path:
        <base_path>/<artifact_id>/
            root.json          # identity, status, scalar fields
            <part_name>        # one file per lazy part — BLOB
            .lock              # advisory fcntl lock file

    """

    def __init__(
        self,
        base_path: Path,
        artifact_type: type[ART],
    ) -> None:
        super().__init__(artifact_type)
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        artifact,
        include="all",
    ) -> None:
        include = self._resolve_part_names(include)
        artifact_dir = self._get_dir(artifact.id)
        artifact_dir.mkdir(parents=True, exist_ok=True)

        with self._lock_artifact(artifact.id, LockMode.EXCLUSIVE):
            self._write_root(artifact_dir, artifact)
            for part_name in include:
                data = getattr(artifact, part_name, None)
                if data is not None:
                    self._write_part(artifact.id, part_name, data)

    def save_part(self, artifact_id, part_name, data) -> None:
        with self._lock_artifact(artifact_id, LockMode.EXCLUSIVE):
            self._write_part(artifact_id, part_name, data)

    def save_many(self, artifacts, include="all") -> None:
        per_artifact_parts = self._resolve_batch_part_names(
            include, batch_len=len(artifacts)
        )
        for artifact, part_names in zip(artifacts, per_artifact_parts, strict=True):
            self.save(artifact, part_names)

    def load(self, artifact_id, include=None, **options):
        raw = self.load_raw(artifact_id, include, **options)
        return self._deserialize_model(raw) if raw is not None else None

    def load_raw(self, artifact_id, include=None, **options) -> dict[str, Any] | None:
        if not self.exists(artifact_id):
            return None
        part_names = self._resolve_part_names(include)
        with self._lock_artifact(artifact_id, LockMode.SHARED):
            return self._read_artifact(artifact_id, part_names)

    def load_many(self, artifact_ids, include=None, **options):
        raw_map = self.load_many_raw(artifact_ids, include, **options)
        return {
            aid: (self._deserialize_model(row) if row is not None else None)
            for aid, row in raw_map.items()
        }

    def load_many_raw(
        self, artifact_ids, include=None, **options
    ) -> dict[str, dict[str, Any] | None]:
        per_artifact_parts = self._resolve_batch_part_names(
            include, batch_len=len(artifact_ids)
        )
        return {
            aid: self.load_raw(aid, inc, **options)
            for aid, inc in zip(artifact_ids, per_artifact_parts, strict=True)
        }

    def load_part(
        self, artifact_id: str, part_name: PartNameT
    ) -> BaseArtifactPart | None:
        with self._lock_artifact(artifact_id, LockMode.SHARED):
            data = self._read_part(artifact_id, part_name)
        return self._part_map[part_name].deserialize(data) if data is not None else None

    def delete_parts(self, artifact_id, parts) -> None:
        if not self.exists(artifact_id):
            raise KeyError(
                f"{self._artifact_type.__name__} with id={artifact_id!r} does not exist."
            )
        part_names = self._resolve_part_names(parts)
        with self._lock_artifact(artifact_id, LockMode.EXCLUSIVE):
            for name in part_names:
                self._get_part_path(artifact_id, name).unlink(missing_ok=True)

    def delete_artifact(self, artifact_id) -> None:
        """
        Remove the entire artifact directory (root + all parts).
        No-op if the artifact does not exist.

        WARNING: Not safe for concurrent deletion — callers must coordinate
        externally if multiple processes may delete the same artifact.
        """
        artifact_dir = self._get_dir(artifact_id)
        if artifact_dir.exists():
            with self._lock_artifact(artifact_id, LockMode.EXCLUSIVE):
                shutil.rmtree(artifact_dir)

    def exists(self, artifact_id: str) -> bool:
        return (self._get_dir(artifact_id) / "root.json").exists()

    def _deserialize_model(self, data: dict[str, Any]) -> ART:
        model = json.loads(data.pop("root"))
        for k, v in data.items():
            if v is not None:
                model[k] = self._part_map[k].deserialize(v)
        return self._artifact_type.model_validate(model)

    def _read_artifact(self, artifact_id, part_names: Sequence[str]) -> dict[str, Any]:
        return {
            "root": self._read_root(artifact_id),
            **{name: self._read_part(artifact_id, name) for name in part_names},
        }

    def _read_root(self, artifact_id) -> str:
        return self._get_root_path(artifact_id).read_text(encoding="utf-8")

    def _read_part(self, artifact_id: str, part_name: PartNameT) -> str | None:
        if part_name not in self._part_map:
            raise ValueError(
                f"Unknown part {part_name} for {self._artifact_type.__name__}. "
                f"Valid parts: {set(self._part_map)}"
            )
        if not self.exists(artifact_id):
            raise KeyError(
                f"{self._artifact_type.__name__} with id={artifact_id!r} does not exist."
            )
        target = self._get_part_path(artifact_id, part_name)
        return target.read_bytes() if target.exists() else None

    def _get_dir(self, artifact_id: str) -> Path:
        return self.base_path / artifact_id

    def _get_root_path(self, artifact_id: str) -> Path:
        return self._get_dir(artifact_id) / "root.json"

    def _get_part_path(self, artifact_id: str, name: str) -> Path:
        return self._get_dir(artifact_id) / f"{name}.bin"

    def _write_root(self, artifact_dir: Path, artifact: ART) -> None:
        """Serialize root fields, excluding lazy parts."""
        self._write_atomic(
            artifact_dir / "root.json",
            artifact.model_dump_json(exclude=self._part_map.keys()).encode("utf-8"),
        )

    def _write_part(
        self,
        artifact_id: str,
        part_name: PartNameT,
        data: BaseArtifactPart,
    ) -> None:
        """
        Atomically write a single part file.
        """
        if part_name not in self._part_map:
            raise ValueError(
                f"Unknown part {part_name} for {self._artifact_type.__name__}. "
                f"Valid parts: {set(self._part_map)}"
            )
        if not self.exists(artifact_id):
            raise KeyError(
                f"{self._artifact_type.__name__} with id={artifact_id!r} does not exist."
            )
        expected_type = self._part_map[part_name]
        if not isinstance(data, expected_type):
            raise TypeError(
                f"Invalid data type for part: {self._artifact_type.__name__}.{part_name} "
                f"Expected [{expected_type.__name__}], got [{type(data).__name__}]."
            )
        self._write_atomic(
            self._get_part_path(artifact_id, part_name), data.serialize()
        )

    def _write_atomic(self, target_path: Path, data: bytes | str) -> None:
        """Write via a sibling .tmp file then rename (atomic on POSIX)."""
        tmp_path = target_path.with_suffix(".tmp")
        try:
            if isinstance(data, bytes):
                tmp_path.write_bytes(data)
            elif isinstance(data, str):
                tmp_path.write_bytes(data)

            tmp_path.replace(target_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            self.logger.error(
                f"Failed writing {target_path}",
            )
            raise

    @contextmanager
    def _lock_artifact(
        self, artifact_id: str, mode: LockMode
    ) -> Generator[Path, None, None]:
        """
        Advisory fcntl lock scoped to a single artifact directory.

        SHARED    → multiple concurrent readers allowed.
        EXCLUSIVE → blocks until all other locks are released.
        """
        artifact_dir = self._get_dir(artifact_id)
        if not artifact_dir.exists():
            raise FileNotFoundError(f"Artifact directory not found: {artifact_id}")

        lock_fd = open(artifact_dir / ".lock", "a")
        try:
            flag = fcntl.LOCK_SH if mode == LockMode.SHARED else fcntl.LOCK_EX
            fcntl.flock(lock_fd.fileno(), flag)
            yield artifact_dir
        finally:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
