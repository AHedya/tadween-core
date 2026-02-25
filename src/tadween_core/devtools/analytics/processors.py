from collections.abc import Generator, Iterable

import polars as pl

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
            return MemoryMetric(session.meta, pl.DataFrame(), id)

        df_main = pl.DataFrame(
            [s for s in session.samples if s.role == MemoryRole.MAIN], orient="row"
        )
        df_child = pl.DataFrame(
            [s for s in session.samples if s.role == MemoryRole.CHILD], orient="row"
        )

        t_max = df_main["time_lapsed"].max() if not df_main.is_empty() else 0
        if not df_child.is_empty():
            t_max = max(t_max, df_child["time_lapsed"].max())

        n_steps = int(t_max / resample_interval) + 1
        grid = (
            pl.int_range(0, n_steps, eager=True).alias("time_lapsed").cast(pl.Float64)
            * resample_interval
        )

        def resample_df(df, val_cols):
            if df.is_empty():
                return grid.to_frame().with_columns(
                    [pl.lit(0.0).alias(c) for c in val_cols]
                )

            # Combine grid and data points to ensure we have all points for interpolation
            df_unique = df.unique(subset="time_lapsed").select(
                ["time_lapsed"] + val_cols
            )

            # Use same dtypes as original data for null placeholders
            schema = df_unique.schema
            combined = pl.concat(
                [
                    grid.to_frame().with_columns(
                        [pl.lit(None, dtype=schema[c]).alias(c) for c in val_cols]
                    ),
                    df_unique,
                ]
            ).sort("time_lapsed")

            # Interpolate
            res = combined.interpolate()

            # Fill edges
            for c in val_cols:
                res = res.with_columns(
                    pl.col(c)
                    .fill_null(strategy="backward")
                    .fill_null(strategy="forward")
                )

            # Filter to keep only the grid points.
            # We use a left join with the grid to ensure we only have the requested intervals
            # and avoid floating point comparison issues with filter.
            return grid.to_frame().join(res, on="time_lapsed", how="left")

        df_main_res = resample_df(df_main, ["usage_mb"]).rename({"usage_mb": "main_mb"})

        if not df_child.is_empty():
            df_child_res = resample_df(df_child, ["usage_mb", "count"]).rename(
                {"usage_mb": "children_mb", "count": "children_count"}
            )
        else:
            df_child_res = grid.to_frame().with_columns(
                [pl.lit(0.0).alias("children_mb"), pl.lit(0).alias("children_count")]
            )

        final_df = df_main_res.join(df_child_res, on="time_lapsed")
        final_df = final_df.with_columns(
            (pl.col("main_mb") + pl.col("children_mb")).alias("total_mb")
        )
        final_df = final_df.with_columns(pl.col("children_count").round(0))

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
        df = pl.DataFrame(session.samples, orient="row")
        return RAMMetric(session.meta, df, id)

    @staticmethod
    def process_batch(
        batch: Iterable[RAMSession], ids: Iterable[int] | None = None
    ) -> Generator[RAMMetric, None, None]:
        # need to document the len(batch) should be the same as len(ids)
        # Either the shortest is considered.

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
        df = pl.DataFrame(session.samples, orient="row")
        # add total `stage_total` column
        # add `offset` column to differentiate
        df = df.with_columns(
            [
                (pl.col("waiting") + pl.col("duration")).alias("stage_total"),
                (pl.col("duration") / (pl.col("waiting") + pl.col("duration")) * 100)
                .round(0)
                .alias("efficiency"),
            ]
        ).with_columns(
            [
                pl.col("stage_total")
                .shift(1)
                .over("task_id")
                .fill_null(0)
                .cum_sum()
                .over("task_id")
                .alias("offset"),
                pl.col("stage_total")
                .sum()
                .over("task_id")
                .alias("task_total_duration"),
            ]
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
