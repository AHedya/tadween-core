import sqlite3
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from tadween_core.types.artifact.tadween import (
    ArtifactStage,
    PartName,
    TadweenArtifact,
)

from .base import ART, BaseArtifactRepo, PartNameT


class SqliteRepo(BaseArtifactRepo[ART, PartNameT]):
    """
    SQLite-based artifact repository.
    Efficiently handles root metadata and heavy parts using a two-table schema.
    Thread-safe implementation with WAL mode enabled.
    """

    def __init__(
        self,
        db_path: str | Path,
        artifact_type: type[ART] | None = None,
    ):
        super().__init__(artifact_type)  # Sets _part_types via parent
        self.db_path = str(db_path)
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        # check_same_thread=False is safe because we use a threading.Lock for writes
        # and SQLite handles concurrent reads well in WAL mode.
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    current_stage TEXT NOT NULL,
                    error_stage TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    metadata TEXT -- JSON blob
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS artifact_parts (
                    artifact_id TEXT NOT NULL,
                    part_name TEXT NOT NULL,
                    data TEXT NOT NULL, -- JSON blob
                    PRIMARY KEY (artifact_id, part_name),
                    FOREIGN KEY (artifact_id) REFERENCES artifacts (id) ON DELETE CASCADE
                )
                """
            )
            conn.commit()

    def create(self, artifact: ART) -> None:
        with self._lock:
            with self._get_connection() as conn:
                try:
                    exclude = self._artifact_type.part_names()
                    root_data = artifact.model_dump(exclude=exclude)

                    # Use Pydantic's JSON serialization for metadata to handle Path objects etc.
                    metadata_json = None
                    if artifact.metadata:
                        metadata_json = artifact.metadata.model_dump_json()

                    conn.execute(
                        """
                        INSERT INTO artifacts (id, current_stage, error_stage, created_at, updated_at, metadata)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            root_data["id"],
                            root_data["current_stage"],
                            root_data.get("error_stage"),
                            root_data["created_at"],
                            root_data["updated_at"],
                            metadata_json,
                        ),
                    )
                except sqlite3.IntegrityError:
                    raise FileExistsError(f"Artifact {artifact.id} already exists")

    def save(
        self,
        artifact: ART,
        include: Iterable[PartNameT] | Literal["all"] | None = "all",
    ) -> None:
        parts_to_save = self._resolve_parts(include)

        with self._lock:
            with self._get_connection() as conn:
                exclude = self._artifact_type.part_names()
                root_data = artifact.model_dump(exclude=exclude)
                updated_at = time.perf_counter()

                metadata_json = None
                if artifact.metadata:
                    metadata_json = artifact.metadata.model_dump_json()

                conn.execute(
                    """
                    INSERT INTO artifacts (id, current_stage, error_stage, created_at, updated_at, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        current_stage = excluded.current_stage,
                        error_stage = excluded.error_stage,
                        updated_at = excluded.updated_at,
                        metadata = excluded.metadata
                    """,
                    (
                        artifact_id := root_data["id"],
                        root_data["current_stage"],
                        root_data.get("error_stage"),
                        root_data["created_at"],
                        updated_at,
                        metadata_json,
                    ),
                )

                for part_name in parts_to_save:
                    part_data = getattr(artifact, part_name)
                    if part_data is not None:
                        conn.execute(
                            """
                            INSERT INTO artifact_parts (artifact_id, part_name, data)
                            VALUES (?, ?, ?)
                            ON CONFLICT(artifact_id, part_name) DO UPDATE SET
                                data = excluded.data
                            """,
                            (artifact_id, part_name, part_data.model_dump_json()),
                        )

    def _get_metadata_type(self) -> type[BaseModel] | None:
        """Introspect the metadata field type from the artifact type."""
        metadata_field = self._artifact_type.model_fields.get("metadata")
        if metadata_field is None:
            return None
        ann = metadata_field.annotation
        if ann is None:
            return None
        # Handle Optional[T] -> T
        if hasattr(ann, "__args__"):
            for arg in ann.__args__:
                if isinstance(arg, type) and issubclass(arg, BaseModel):
                    return arg
        elif isinstance(ann, type) and issubclass(ann, BaseModel):
            return ann
        return None

    def load(
        self,
        artifact_id: str,
        include: Iterable[PartNameT] | Literal["all"] | None = None,
    ) -> ART:
        parts_to_load = self._resolve_parts(include)

        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
            ).fetchone()
            if not row:
                raise KeyError(f"Artifact not found: {artifact_id}")

            root_dict = dict(row)
            if root_dict["metadata"]:
                metadata_type = self._get_metadata_type()
                if metadata_type is not None:
                    root_dict["metadata"] = metadata_type.model_validate_json(
                        root_dict["metadata"]
                    )

            artifact = self._artifact_type.model_validate(root_dict)

            if parts_to_load:
                placeholders = ",".join("?" for _ in parts_to_load)
                parts_rows = conn.execute(
                    f"SELECT part_name, data FROM artifact_parts "
                    f"WHERE artifact_id = ? AND part_name IN ({placeholders})",
                    (artifact_id, *parts_to_load),
                ).fetchall()

                for p_row in parts_rows:
                    p_name, p_data = p_row["part_name"], p_row["data"]
                    model_cls = self._part_types[p_name]
                    setattr(artifact, p_name, model_cls.model_validate_json(p_data))

            return artifact

    def save_part(
        self, artifact_id: str, part_name: PartNameT, data: BaseModel
    ) -> None:
        if part_name not in self._part_types:
            raise ValueError(f"Unknown part: {part_name}")

        with self._lock:
            with self._get_connection() as conn:
                exists = conn.execute(
                    "SELECT 1 FROM artifacts WHERE id = ?", (artifact_id,)
                ).fetchone()
                if not exists:
                    raise FileNotFoundError(f"Artifact {artifact_id} not found")

                conn.execute(
                    """
                    INSERT INTO artifact_parts (artifact_id, part_name, data)
                    VALUES (?, ?, ?)
                    ON CONFLICT(artifact_id, part_name) DO UPDATE SET
                        data = excluded.data
                    """,
                    (artifact_id, part_name, data.model_dump_json()),
                )

    def load_part(self, artifact_id: str, part_name: PartNameT) -> BaseModel | None:
        if part_name not in self._part_types:
            raise ValueError(f"Unknown part: {part_name}")

        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT data FROM artifact_parts WHERE artifact_id = ? AND part_name = ?",
                (artifact_id, part_name),
            ).fetchone()

            if not row:
                return None

            model_cls = self._part_types[part_name]
            return model_cls.model_validate_json(row["data"])

    def exists(self, artifact_id: str) -> bool:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM artifacts WHERE id = ?", (artifact_id,)
            ).fetchone()
            return row is not None

    def delete(self, artifact_id: str) -> None:
        with self._lock:
            with self._get_connection() as conn:
                conn.execute("DELETE FROM artifacts WHERE id = ?", (artifact_id,))

    def _resolve_parts(
        self, include: Iterable[PartNameT] | Literal["all"] | None
    ) -> set[PartNameT]:
        if include == "all":
            return set(self._part_types.keys())
        if include is None:
            return set()
        return set(include)


# ---------------------------------------------------------------------------
# Tadween-specific subclass — zero generics for end users
# ---------------------------------------------------------------------------


class TadweenSqliteRepo(SqliteRepo[TadweenArtifact, PartName]):
    """
    Ready-to-use SQLite repo for TadweenArtifact.

        repo = TadweenSqliteRepo(db_path=Path("/data/tadween.db"))
    """

    def __init__(self, db_path: str | Path) -> None:
        super().__init__(db_path, artifact_type=TadweenArtifact)

    def update_status(
        self,
        artifact_id: str,
        stage: ArtifactStage,
        error_stage: ArtifactStage | None = None,
    ) -> None:
        """
        Lightweight status patch — touches only the artifacts table,
        leaving all parts untouched.

        Kept here (not on the generic base) because ArtifactStage is
        Tadween-specific.
        """
        with self._lock:
            with self._get_connection() as conn:
                result = conn.execute(
                    """
                    UPDATE artifacts
                    SET current_stage = ?,
                        error_stage   = ?,
                        updated_at    = ?
                    WHERE id = ?
                    """,
                    (stage, error_stage, time.perf_counter(), artifact_id),
                )
                if result.rowcount == 0:
                    raise FileNotFoundError(f"Artifact {artifact_id} not found")
