import logging
import math
from collections.abc import Iterable

import polars as pl
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter

from .schemas import MemoryMetric, RAMMetric, RuntimeMetric  # noqa

logger = logging.getLogger(__name__)


class RAMPlotter:
    def __init__(self, metrics: Iterable[RAMMetric]):
        self.metrics = tuple(metrics)

    def plot(
        self,
        title: str = "RAM usage plot",
        indexes: list[int] | None = None,
        show_peak: bool = False,
        show_average: bool = False,
        include_children: bool = False,
    ):
        """_summary_

        Args:
            indexes (list[int] | None, optional): metrics indexes to plot. Defaults to None, means all.

        """
        plt.figure(figsize=(12, 6))
        indexes = range(len(self.metrics)) if indexes is None else set(indexes)

        for idx in indexes:
            if not (0 <= idx < len(self.metrics)):
                logger.warning(
                    f"Index: {idx} out of metrics range: [0,{len(self.metrics)}["
                )
                continue

            metric = self.metrics[idx]

            (line,) = plt.plot(
                metric.data["time_lapsed"],
                metric.data["total_mb"],
                label=f"S{idx}: {metric.meta.title}",
            )
            color = line.get_color()
            if show_peak:
                plt.axhline(y=metric.peak, color=color, linestyle="--", alpha=0.3)
            if show_average:
                plt.axhline(y=metric.avg, color=color, linestyle=":", alpha=0.2)
            plt.text(
                metric.data["time_lapsed"].item(-1),
                metric.data["total_mb"].item(-1),
                f"S{idx}",
                color=color,
                va="center",
                fontsize=12,
            )
            if include_children:
                plt.plot(
                    metric.data["time_lapsed"],
                    metric.data["children_mb"],
                    color=color,
                    linestyle="-.",
                    alpha=0.4,
                    label=f"S{idx} Children",
                )

        plt.title(title)
        plt.xlabel("Seconds Elapsed (Relative)")
        plt.ylabel("Usage (MB)")
        plt.legend(loc="upper left", bbox_to_anchor=(1, 1))
        plt.grid(True, alpha=0.2)
        plt.tight_layout()
        plt.show()


class MemoryPlotter:
    def __init__(self, metrics: Iterable[MemoryMetric]):
        self.metrics = tuple(metrics)

    def plot(
        self,
        title: str = "Memory usage plot",
        indexes: list[int] | None = None,
        show_peak: bool = False,
        show_average: bool = False,
        show_details: bool = False,
        include_children: bool = False,
    ):
        fig, ax = plt.subplots(figsize=(12, 6))
        active = range(len(self.metrics)) if indexes is None else set(indexes)

        detail_handles: list[Patch] = []

        for idx in active:
            if not (0 <= idx < len(self.metrics)):
                logger.warning(
                    f"Index: {idx} out of metrics range: [0,{len(self.metrics)}["
                )
                continue

            metric = self.metrics[idx]
            t = metric.data["time_lapsed"]
            mem = metric.data["total_mb"]

            (line,) = ax.plot(t, mem, label=f"S{idx}: {metric.meta.title}")
            color = line.get_color()

            if show_peak:
                ax.axhline(y=metric.peak, color=color, linestyle="--", alpha=0.3)
                ax.annotate(
                    f"peak {metric.peak:.1f} MB",
                    xy=(t.item(0), metric.peak),
                    xytext=(4, 3),
                    textcoords="offset points",
                    color=color,
                    fontsize=8,
                    alpha=0.6,
                )
            if show_average:
                ax.axhline(y=metric.avg, color=color, linestyle=":", alpha=0.2)

            ax.text(
                t.item(-1),
                mem.item(-1),
                f" S{idx}",
                color=color,
                va="center",
                fontsize=11,
                fontweight="bold",
            )

            if include_children:
                ax.plot(
                    t,
                    metric.data["children_mb"],
                    color=color,
                    linestyle="-.",
                    alpha=0.4,
                    label=f"S{idx} Children",
                )

            if show_details:
                detail_handles.append(
                    Patch(color=color, label=self._detail_label(idx, metric), alpha=0.8)
                )

        if show_details and detail_handles:
            ax.add_artist(self._build_primary_legend(ax))
            self._build_details_legend(ax, detail_handles)
        else:
            self._build_primary_legend(ax)

        ax.set_title(title)
        ax.set_xlabel("Seconds Elapsed (Relative)")
        ax.set_ylabel("Usage (MB)")
        ax.grid(True, alpha=0.2)

        if show_details and detail_handles:
            # Reserve extra bottom margin so the details legend doesn't clip.
            # tight_layout alone doesn't account for out-of-axes legend boxes.
            fig.tight_layout()
            fig.subplots_adjust(bottom=0.22)
        else:
            fig.tight_layout()

        plt.show()

    @staticmethod
    def _detail_label(idx: int, metric: MemoryMetric) -> str:
        meta = metric.meta
        parts = [
            f"S{idx}",
            f"main={meta.main_collector}",
            f"children={meta.children_collector}",
            f"pid={meta.pid}",
        ]
        return "  -  ".join(parts)

    def _build_primary_legend(self, ax):
        return ax.legend(
            loc="upper left",
            bbox_to_anchor=(1.01, 1.0),
            borderaxespad=0,
            title="Sessions",
            fontsize=9,
        )

    def _build_details_legend(self, ax, handles: list):
        """Placed inside the axes at the bottom so its width doesn't shrink the plot."""
        legend = ax.legend(
            handles=handles,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.28),
            ncols=max(1, len(handles)),
            borderaxespad=0,
            title="Details",
            fontsize=8,
            title_fontsize=9,
            handlelength=1.0,
            handleheight=0.9,
            framealpha=0.9,
            edgecolor="#cccccc",
            fancybox=False,
        )
        for text in legend.get_texts():
            text.set_fontfamily("monospace")
        return legend


class RuntimePlotter:
    def __init__(self, metrics: Iterable[RuntimeMetric]):
        self.metrics = tuple(metrics)

    def plot(
        self,
        title: str = "Runtime Execution Plot",
        indexes: list[int] | None = None,
        sort_by_duration: bool = False,
        show_details: bool = False,
        show_stage_stats: bool = True,
        share_x: bool = False,
        dpi: int = 300,
    ) -> plt.Figure | None:
        target = range(len(self.metrics)) if indexes is None else indexes
        metrics = [self.metrics[i] for i in target if 0 <= i < len(self.metrics)]
        if not metrics:
            logger.warning("No valid metrics to plot.")
            return None

        n_rows = len(metrics)
        n_cols = 2 if show_stage_stats else 1
        width_ratios = [4, 1] if show_stage_stats else [1]

        fig, axes = plt.subplots(
            nrows=n_rows,
            ncols=n_cols,
            figsize=(18, sum(self._row_height(m) for m in metrics)),
            dpi=dpi,
            gridspec_kw={"width_ratios": width_ratios},
        )
        if n_rows == 1 and n_cols == 1:
            axes = axes[None, None]
        elif n_rows == 1:
            axes = axes[None, :]
        elif n_cols == 1:
            axes = axes[:, None]

        colormap = plt.get_cmap("tab10")

        for row_idx, metric in enumerate(metrics):
            df = (
                metric.data.sort("task_total_duration", descending=True)
                if sort_by_duration
                else metric.data
            )
            stages = df["stage"].unique(maintain_order=True).to_list()
            colors = {stage: colormap(i % 10) for i, stage in enumerate(stages)}

            self._plot_gantt(axes[row_idx, 0], metric, df, colors, show_details)

            if show_stage_stats:
                self._plot_stage_distribution(axes[row_idx, 1], df, colors)

        if share_x:
            gantt_axes = axes[:, 0]
            x_min = min(ax.get_xlim()[0] for ax in gantt_axes)
            x_max = max(ax.get_xlim()[1] for ax in gantt_axes)
            # Infer tick step from the first axis
            ticks = gantt_axes[0].get_xticks()
            if len(ticks) >= 2:
                step = ticks[1] - ticks[0]
                x_max = math.ceil(x_max / step) * step
            for ax in gantt_axes:
                ax.set_xlim(x_min, x_max)

        fig.suptitle(title, fontsize=14, fontweight="bold")
        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_height(metric: RuntimeMetric) -> float:
        return max(4.0, metric.data["task_id"].n_unique() * 0.4)

    @staticmethod
    def _plot_gantt(
        ax: Axes,
        metric: RuntimeMetric,
        df: pl.DataFrame,
        colors: dict[str, tuple],
        show_details: bool,
    ) -> None:
        tasks = df["task_id"].unique(maintain_order=True).to_list()
        task_y = {tid: i for i, tid in enumerate(tasks)}

        for row in df.iter_rows(named=True):
            y = task_y[row["task_id"]]
            c = colors[row["stage"]]
            ax.barh(
                y, row["waiting"], left=row["offset"], color=c, alpha=0.3, height=0.7
            )
            ax.barh(
                y,
                row["duration"],
                left=row["offset"] + row["waiting"],
                color=c,
                alpha=1.0,
                height=0.7,
                edgecolor="black",
                linewidth=0.2,
            )

        if show_details:
            RuntimePlotter._annotate_efficiencies(ax, df, task_y)

        ax.set_yticks(range(len(tasks)))
        ax.set_yticklabels(tasks, fontsize=8)
        ax.set_xlabel("Time (s)")
        ax.set_title(
            f"Session {metric.id} — {metric.meta.title}", loc="left", fontweight="bold"
        )
        ax.legend(
            handles=[Patch(color=c, label=s) for s, c in colors.items()],
            fontsize=8,
            loc="lower left",
        )
        ax.xaxis.grid(True, linestyle="--", color="grey", alpha=0.5, zorder=0)
        ax.set_axisbelow(True)
        x_max = ax.get_xlim()[1]
        tick_spacing = ax.get_xticks()
        if len(tick_spacing) >= 2:
            step = tick_spacing[1] - tick_spacing[0]

            ax.set_xlim(right=math.ceil(x_max / step) * step)

    @staticmethod
    def _annotate_efficiencies(
        ax: Axes,
        df: pl.DataFrame,
        task_y: dict[str, int],
    ) -> None:
        grouped = df.group_by("task_id").agg(
            [
                (pl.col("offset") + pl.col("waiting") + pl.col("duration"))
                .max()
                .alias("x_end"),
                pl.col("efficiency").alias("efficiencies"),
            ]
        )
        for row in grouped.iter_rows(named=True):
            label = ", ".join(f"{e:.0f}" for e in row["efficiencies"])
            ax.text(
                row["x_end"],
                task_y[row["task_id"]],
                f"  [{label}]%",
                va="center",
                ha="left",
                fontsize=7,
                color="dimgray",
            )

    @staticmethod
    def _plot_stage_distribution(
        ax: Axes,
        df: pl.DataFrame,
        colors: dict[str, tuple],
    ) -> None:
        stats = df.group_by("stage").agg(
            [
                pl.col("waiting").sum().alias("total_wait"),
                pl.col("duration").sum().alias("total_dur"),
            ]
        )
        total_volume = stats["total_wait"].sum() + stats["total_dur"].sum()

        for i, row in enumerate(stats.to_dicts()):
            c = colors[row["stage"]]
            w = row["total_wait"] / total_volume
            d = row["total_dur"] / total_volume
            ax.bar(i, w, color=c, alpha=0.3)
            ax.bar(i, d, bottom=w, color=c, alpha=1.0, edgecolor="black", linewidth=0.3)
            efficiency = d / (w + d) * 100 if (w + d) > 0 else 0
            ax.text(
                i,
                w + d + 0.01,
                f"{efficiency:.0f}%",
                ha="center",
                va="bottom",
                fontsize=8,
            )

        stage_names = stats["stage"].to_list()
        ax.set_xticks(range(len(stage_names)))
        ax.set_xticklabels(stage_names, fontsize=8, rotation=15, ha="right")
        ax.set_ylim(0, 1.1)
        ax.set_ylabel("Share of Total Time")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
        ax.set_title("Stage Distribution", fontsize=10)
