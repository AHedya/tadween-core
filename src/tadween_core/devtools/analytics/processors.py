from collections.abc import Generator, Iterable

import pandas as pd

from .loaders import (  # noqa
    load_memory_sessions,
    load_ram_sessions,
    load_runtime_sessions,
)
from .schemas import (  # noqa
    MemoryMetric,
    MemoryRole,
    MemorySession,
    RAMMetric,
    RAMSession,
    RuntimeMetric,
    RuntimeSession,
)


class MemorySessionProcessor:
    @staticmethod
    def process_session(
        session: MemorySession,
        id: str | int | None = None,
        resample_interval: float = 0.05,
    ) -> MemoryMetric:
        if not session.samples:
            return MemoryMetric(session.meta, pd.DataFrame(), id)

        df_main = pd.DataFrame(
            [s.to_dict() for s in session.samples if s.role == MemoryRole.MAIN]
        )
        df_child = pd.DataFrame(
            [s.to_dict() for s in session.samples if s.role == MemoryRole.CHILD]
        )

        t_max = df_main["time_lapsed"].max() if not df_main.empty else 0
        if not df_child.empty:
            t_max = max(t_max, df_child["time_lapsed"].max())

        n_steps = int(t_max / resample_interval) + 1
        grid = pd.Series(
            pd.RangeIndex(0, n_steps) * resample_interval,
            dtype=float,
            name="time_lapsed",
        )

        def resample_df(df, val_cols):
            if df.empty:
                return grid.to_frame().assign(**dict.fromkeys(val_cols, 0.0))

            df_unique = df.drop_duplicates(subset="time_lapsed")[
                ["time_lapsed"] + val_cols
            ]

            combined = pd.concat(
                [
                    grid.to_frame().assign(**dict.fromkeys(val_cols)),
                    df_unique,
                ],
                ignore_index=True,
            ).sort_values("time_lapsed")

            res = combined.interpolate()

            for c in val_cols:
                res[c] = res[c].fillna(method="bfill").fillna(method="ffill")

            return grid.to_frame().merge(res, on="time_lapsed", how="left")

        df_main_res = resample_df(df_main, ["usage_mb"]).rename(
            columns={"usage_mb": "main_mb"}
        )

        if not df_child.empty:
            df_child_res = resample_df(df_child, ["usage_mb", "count"]).rename(
                columns={"usage_mb": "children_mb", "count": "children_count"}
            )
        else:
            df_child_res = grid.to_frame().assign(children_mb=0.0, children_count=0)

        final_df = df_main_res.merge(df_child_res, on="time_lapsed")
        final_df = final_df.assign(
            total_mb=final_df["main_mb"] + final_df["children_mb"]
        )
        final_df["children_count"] = final_df["children_count"].round(0)

        return MemoryMetric(session.meta, final_df, id)

    @staticmethod
    def process_batch(
        batch: Iterable[MemorySession], ids: Iterable[int] | None = None
    ) -> Generator[MemoryMetric, None, None]:
        if ids is None:
            for id, m in enumerate(batch):
                yield MemorySessionProcessor.process_session(session=m, id=id)
        else:
            for id, m in zip(ids, batch, strict=False):
                yield MemorySessionProcessor.process_session(session=m, id=id)

    @staticmethod
    def from_file(fp: str) -> Generator[MemoryMetric, None, None]:
        for idx, session in enumerate(load_memory_sessions(fp)):
            yield MemorySessionProcessor.process_session(session, id=idx)


class RAMSessionProcessor:
    @staticmethod
    def process_session(
        session: RAMSession,
        id: str | int | None = None,
    ) -> RAMMetric:
        df = pd.DataFrame([s.to_dict() for s in session.samples])
        return RAMMetric(session.meta, df, id)

    @staticmethod
    def process_batch(
        batch: Iterable[RAMSession], ids: Iterable[int] | None = None
    ) -> Generator[RAMMetric, None, None]:
        if ids is None:
            for id, m in enumerate(batch):
                yield RAMSessionProcessor.process_session(session=m, id=id)
        else:
            for id, m in zip(ids, batch, strict=False):
                yield RAMSessionProcessor.process_session(session=m, id=id)

    @staticmethod
    def from_file(fp: str) -> Generator[RAMMetric, None, None]:
        for idx, session in enumerate(load_ram_sessions(fp)):
            yield RAMSessionProcessor.process_session(session, id=idx)


class RuntimeSessionProcessor:
    @staticmethod
    def process_session(
        session: RuntimeSession,
        id: str | int | None = None,
    ) -> RuntimeMetric:
        df = pd.DataFrame([s.to_dict() for s in session.samples])
        df["stage_total"] = df["waiting"] + df["duration"]
        df["efficiency"] = ((df["duration"] / df["stage_total"]) * 100).round(0)
        df["offset"] = df.groupby("task_id")["stage_total"].shift(1).fillna(0)
        df["offset"] = df.groupby("task_id")["offset"].cumsum()
        df["task_total_duration"] = df.groupby("task_id")["stage_total"].transform(
            "sum"
        )

        return RuntimeMetric(session.meta, df, id)

    @staticmethod
    def process_batch(
        batch: Iterable[RuntimeSession], ids: Iterable[int] | None = None
    ) -> Generator[RuntimeMetric, None, None]:
        if ids is None:
            for id, m in enumerate(batch):
                yield RuntimeSessionProcessor.process_session(session=m, id=id)
        else:
            for id, m in zip(ids, batch, strict=False):
                yield RuntimeSessionProcessor.process_session(session=m, id=id)

    @staticmethod
    def from_file(fp: str) -> Generator[RuntimeMetric, None, None]:
        for idx, session in enumerate(load_runtime_sessions(fp)):
            yield RuntimeSessionProcessor.process_session(session, id=idx)
