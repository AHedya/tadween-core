import functools
import sqlite3
import threading
from collections import defaultdict
from collections.abc import Sequence
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from tadween_core.repo.base import ART, BaseArtifactRepo, CriteriaDict, PartNameT
from tadween_core.types.artifact.base import (
    RootModel,
)

_PY_TO_SQLITE: dict[type, str] = {
    str: "TEXT",
    int: "INTEGER",
    float: "REAL",
    bool: "INTEGER",
    UUID: "TEXT",
    Enum: "TEXT",
    bytes: "BLOB",
}


def _write_locked(method):
    """Serialize write operations using the instance's _write_lock."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._write_lock:
            return method(self, *args, **kwargs)

    return wrapper


class SqliteRepo(BaseArtifactRepo[ART, PartNameT]):
    """
    SQLite-based SQLite implementation of :class:`BaseArtifactRepo`
    """

    def __init__(
        self,
        db_path: str | Path,
        artifact_type: type[ART],
    ):
        super().__init__(artifact_type)
        self.db_path = str(db_path)
        self._write_lock = threading.Lock()
        self._root_table = f"{self._artifact_type.__name__.lower()}_root"
        self._root_type: type[RootModel] = artifact_type.model_fields["root"].annotation
        self._root_type_map = self._root_type.get_field_type_map()

        self._init_db()
        self._root_cols: str | None = None
        self._root_cols_placeholders: str | None = None
        self._set_constants()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA busy_timeout=5000;")

            columns = ["id TEXT PRIMARY KEY"]
            for name, py_type in self._root_type_map.items():
                if name == "id":
                    continue
                columns.append(f"{name} {_PY_TO_SQLITE[py_type]} NOT NULL")

            for name in self._eager_map:
                columns.append(f"{name} TEXT NOT NULL")

            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self._root_table} ({', '.join(columns)})"
            )

            # Build heavy parts Tables
            for part_name in self._part_map:
                table_name = self._get_part_table_name(part_name)
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        artifact_id TEXT PRIMARY KEY,
                        data TEXT NOT NULL,
                        FOREIGN KEY (artifact_id) REFERENCES {self._root_table} (id) ON DELETE CASCADE
                    )
                """)

    def _get_part_table_name(self, part_name: str):
        return f"{self._artifact_type.__name__.lower()}_part_{part_name.lower()}"

    def _trim_part_table_name(self, table_name: str) -> str:
        prefix = f"{self._artifact_type.__name__.lower()}_part_"
        if not table_name.startswith(prefix):
            raise ValueError(
                f"Table name {table_name!r} does not start with expected prefix {prefix!r}"
            )
        return table_name[len(prefix) :]

    def _set_constants(self):
        cols = list(
            self._artifact_type.model_fields["root"].annotation.model_fields.keys()
        )
        cols.extend(self._eager_map.keys())
        self._root_cols = ", ".join(cols)
        self._root_cols_placeholders = ", ".join(f":{k}" for k in cols)

        self._parts_tables = [
            self._get_part_table_name(part) for part in self._part_map.keys()
        ]

    def _extract_root_row(self, artifact: ART) -> dict[str, str]:
        """Flattens the artifact into a dictionary matching the root table columns."""
        # we use model dump because the schema expects columns
        row = artifact.root.model_dump(mode="json")
        # Eager models as JSON string
        for name in self._eager_map:
            val = getattr(artifact, name)
            if val is not None:
                # here we use dump_json as eagers are saved as strings
                row[name] = val.model_dump_json()
        return row

    def _deserialize_sql_row(self, row: dict[str, Any]) -> ART:
        root_fields = self._artifact_type.model_fields[
            "root"
        ].annotation.model_fields.keys()

        root_data = {}
        eager_data = {}
        lazy_data = {}

        for f in row:
            if f in root_fields:
                root_data[f] = row[f]
            elif f in self._eager_map.keys():
                eager_data[f] = row[f]
            else:
                part = self._trim_part_table_name(f)
                if part in self._part_map.keys():
                    lazy_data[part] = row[f]
                else:
                    self.logger.warning(f"Unrecognized field: {f}")

        model_dict = {}

        # rebuild root
        model_dict["root"] = self._artifact_type.model_fields[
            "root"
        ].annotation.model_validate(root_data)

        for k, v in eager_data.items():
            model_dict[k] = self._eager_map.get(k).model_validate_json(v)

        for k, v in lazy_data.items():
            if v is None:
                continue
            model_dict[k] = self._part_map.get(k).deserialize(v)
        return self._artifact_type.model_validate(model_dict)

    @_write_locked
    def save_many(self, artifacts, include="all"):
        include = self._resolve_batch_part_names(include, batch_len=len(artifacts))

        root_rows = []
        # part_name -> list[tuple(artifact_id,data)]. Data is json string for root, and blobs for the eager.
        part_table_rows: dict[str, list[tuple[str, str | bytes]]] = defaultdict(list)

        for art, inc in zip(artifacts, include, strict=True):
            root_rows.append(self._extract_root_row(art))
            for part_included in inc:
                value = getattr(art, part_included)
                if value is None:
                    self.logger.warning(
                        f'{self._artifact_type.__name__}(id="{art.id}") '
                        f"is trying to save part [{part_included}] with `None` value. Skipping"
                    )
                    continue
                part_table_rows[part_included].append((art.id, value.serialize()))

        if not root_rows:
            return

        root_stmt = f"""
        INSERT OR REPLACE INTO {self._root_table} ({self._root_cols})
        values ({self._root_cols_placeholders})
        """

        with sqlite3.connect(self.db_path) as con:
            con.executemany(root_stmt, root_rows)

            for part, batch in part_table_rows.items():
                if not batch:
                    continue
                # insert to a table at a time
                con.executemany(
                    f"""
                    INSERT INTO {self._get_part_table_name(part)} (artifact_id, data) VALUES (?, ?)
                    ON CONFLICT(artifact_id) DO UPDATE SET data=excluded.data
                """,
                    batch,
                )

    def save(self, artifact, include="all"):
        include = self._resolve_part_names(include)
        self.save_many(artifacts=[artifact], include=[include])

    def load(self, artifact_id, include=None, **options) -> ART | None:
        include = self._resolve_part_names(include)
        res = self._load([artifact_id], [include], True, **options)
        return res.get(artifact_id)

    def load_raw(self, artifact_id, include=None, **options) -> dict[str, Any] | None:
        include = self._resolve_part_names(include)
        res = self._load([artifact_id], [include], False, **options)
        return res.get(artifact_id)

    def load_many(self, artifact_ids, include=None, **options) -> dict[str, ART | None]:
        include = self._resolve_batch_part_names(include, len(artifact_ids))
        return self._load(artifact_ids, include, True, **options)

    def load_many_raw(
        self, artifact_ids, include=None, **options
    ) -> dict[str, dict[str, Any] | None]:
        include = self._resolve_batch_part_names(include, len(artifact_ids))
        return self._load(artifact_ids, include, False, **options)

    def exists(self, artifact_id: str) -> bool:
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                f"SELECT 1 FROM {self._root_table} WHERE id = ? LIMIT 1",
                (artifact_id,),
            ).fetchone()
        return row is not None

    def load_part(
        self,
        artifact_id,
        part_name,
    ):
        if part_name not in self._part_map:
            raise ValueError(
                f"Unknown part {part_name} for {self._artifact_type.__name__}. "
                f"Valid parts: {set(self._part_map)}"
            )
        if not self.exists(artifact_id):
            raise KeyError(
                f"{self._artifact_type.__name__} with id={artifact_id!r} does not exist."
            )
        table = self._get_part_table_name(part_name)
        with sqlite3.connect(self.db_path) as con:
            row = con.execute(
                f"SELECT data FROM {table} WHERE artifact_id = ?",
                (artifact_id,),
            ).fetchone()
        if row is None:
            return None
        return self._part_map[part_name].deserialize(row[0])

    @_write_locked
    def save_part(
        self,
        artifact_id,
        part_name,
        data,
    ) -> None:
        if part_name not in self._part_map:
            raise ValueError(
                f"Unknown part {part_name!r} for {self._artifact_type.__name__}. "
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

        table = self._get_part_table_name(part_name)
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                f"""
                INSERT INTO {table} (artifact_id, data) VALUES (?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET data = excluded.data
                """,
                (artifact_id, data.serialize()),
            )

    @_write_locked
    def delete_artifact(self, artifact_id: str) -> None:
        with sqlite3.connect(self.db_path) as con:
            con.execute("PRAGMA foreign_keys = ON;")
            con.execute(
                f"DELETE FROM {self._root_table} WHERE id = ?",
                (artifact_id,),
            )

    @_write_locked
    def delete_parts(
        self,
        artifact_id,
        parts,
    ) -> None:
        part_names = self._resolve_part_names(parts)
        if not self.exists(artifact_id):
            raise KeyError(
                f"{self._artifact_type.__name__} with id={artifact_id!r} does not exist."
            )
        with sqlite3.connect(self.db_path) as con:
            for part_name in part_names:
                table = self._get_part_table_name(part_name)
                con.execute(
                    f"DELETE FROM {table} WHERE artifact_id = ?",
                    (artifact_id,),
                )

    def _get_matching_ids(
        self, criteria: CriteriaDict, max_len: int | None = None
    ) -> list[str]:
        self._validate_criteria(criteria)

        clauses = []
        params = []
        op_map = {
            "eq": "=",
            "ne": "!=",
            "gt": ">",
            "lt": "<",
            "ge": ">=",
            "le": "<=",
        }

        if not criteria:
            sql = f"SELECT id FROM {self._root_table}"
        else:
            for field, crit_val in criteria.items():
                if (
                    isinstance(crit_val, tuple)
                    and len(crit_val) == 2
                    and isinstance(crit_val[1], str)
                ):
                    expected_val, op_str = crit_val
                else:
                    expected_val, op_str = crit_val, "eq"

                op = op_map.get(op_str)
                if not op:
                    raise ValueError(f"Unsupported operator: {op_str}")

                clauses.append(f"{field} {op} ?")

                if isinstance(expected_val, UUID):
                    params.append(str(expected_val))
                elif isinstance(expected_val, Enum):
                    params.append(expected_val.value)
                else:
                    params.append(expected_val)

            where_sql = " AND ".join(clauses)
            sql = f"SELECT id FROM {self._root_table} WHERE {where_sql}"

        if max_len is not None:
            sql += " LIMIT ?"
            params.append(max_len)

        with sqlite3.connect(self.db_path) as con:
            rows = con.execute(sql, params).fetchall()
        return [row[0] for row in rows]

    def filter(
        self,
        criteria: CriteriaDict,
        include=None,
        max_len: int | None = None,
        **options: Any,
    ) -> dict[str, ART]:
        matching_ids = self._get_matching_ids(criteria, max_len)

        if isinstance(include, Sequence):
            include = [include] * len(matching_ids)
        results = self.load_many(matching_ids, include, **options)
        return {k: v for k, v in results.items() if v is not None}

    def list_parts(
        self,
        criteria: CriteriaDict,
        max_len: int | None = None,
    ) -> dict[str, dict[str, bool]]:
        matching_ids = self._get_matching_ids(criteria, max_len)
        if not matching_ids:
            return {}

        select_parts = []
        for part_name in self._part_map.keys():
            table = self._get_part_table_name(part_name)
            select_parts.append(
                f"EXISTS(SELECT 1 FROM {table} WHERE artifact_id = id) as has_{part_name}"
            )

        placeholders = ",".join(["?"] * len(matching_ids))
        parts_sql = ", ".join(select_parts)
        sql = f"SELECT id, {parts_sql} FROM {self._root_table} WHERE id IN ({placeholders})"

        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(sql, matching_ids).fetchall()

        result = {}
        for row in rows:
            aid = row["id"]
            parts_info = {}
            for part_name in self._part_map.keys():
                parts_info[part_name] = bool(row[f"has_{part_name}"])
            result[aid] = parts_info

        return result

    def _load(
        self,
        artifact_ids: Sequence[str],
        include: list[list[str]],
        deserialize: bool = True,
        **options,
    ) -> dict[str, ART | dict[str, Any] | None]:
        if not artifact_ids:
            return {}

        # Build the CTE rows: ('id1', 1, 1), ('id0', 0, 0), ('id2', 0, 1)
        cte_rows = []
        cte_params = []

        for _id, _include in zip(artifact_ids, include, strict=True):
            flags = [1 if pt in _include else 0 for pt in self._part_map.keys()]
            placeholders = ", ".join(["?"] * (1 + len(self._part_map.keys())))
            cte_rows.append(f"({placeholders})")
            cte_params.extend([_id] + flags)

        cte_columns = ["id"] + [f"need_{c}" for c in self._parts_tables]
        cte_col_sql = ", ".join(cte_columns)
        cte_values_sql = ", ".join(cte_rows)

        # Build conditional subquery per part table
        parts_selects = []
        for p in self._parts_tables:
            parts_selects.append(
                f"CASE n.need_{p} WHEN 1 "
                f"THEN (SELECT data FROM {p} WHERE artifact_id = p.id) "
                f"END AS {p}"
            )
        parts_sql = ",\n       ".join(parts_selects)
        sql = f"""
            WITH needed({cte_col_sql}) AS (VALUES {cte_values_sql})
            SELECT p.*,
                    {parts_sql}
            FROM {self._root_table} p
            JOIN needed n ON n.id = p.id
        """

        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            res = con.execute(sql, cte_params)
            res = [dict(row) for row in res.fetchall()]

        # lookup
        rows_by_id = {row["id"]: row for row in res}
        result = {_id: rows_by_id.get(_id) for _id in artifact_ids}

        if deserialize:
            result = {
                _id: self._deserialize_sql_row(row) if row is not None else None
                for _id, row in result.items()
            }
        return result
