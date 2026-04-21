import logging
import multiprocessing as mp
import os
import time
from collections.abc import Generator
from contextlib import contextmanager
from multiprocessing.synchronize import Event
from typing import Literal, TypeAlias

import psutil

from tadween_core.devtools.utils import FileMode, format_mb, reserve_path

from ..schemas import (
    MEMORY_HEADER,
    MEMORY_SEP,
    MemoryMetadata,
    MemoryRole,
)

MemoryCollectorType: TypeAlias = Literal["rss", "pss", "uss", "vms"]
_VALID_COLLECTORS = ("rss", "pss", "uss", "vms")

_MEMORY_READERS = {
    "rss": lambda proc: proc.memory_info().rss,
    "vms": lambda proc: proc.memory_info().vms,
    "uss": lambda proc: proc.memory_full_info().uss,
    "pss": lambda proc: proc.memory_full_info().pss,
}


class MemoryCollector:
    def __init__(self):
        self._ctx = mp.get_context("spawn")
        self._event: Event | None = None
        self._worker_process: mp.Process | None = None

    def start(
        self,
        title: str | None = None,
        main_interval: float = 0.2,
        children_interval: float = 0.5,
        main_metric_collector: MemoryCollectorType = "pss",
        children_metric_collector: MemoryCollectorType | None = None,
        log_dir: str = "logs",
        file_name: str = "ram_sessions.log",
        file_mode: FileMode = "append",
        include_children: bool = True,
        description: str | None = None,
        include_collector: bool = False,
    ):
        """
        Starts the background monitoring process.

        Args:
            title: Session name for the log header.
            main_interval: How often (seconds) to sample the main process.
            children_interval: How often (seconds) to sample child processes.
            main_metric_collector: Metric type for the main process.
            children_metric_collector: Metric type for children (defaults to main_metric_collector).
            log_dir: Folder where logs will be saved.
            file_name: Name of the log file.
            file_mode: 'append' to keep old logs, 'write' to overwrite.
            include_children: If True, tracks all subprocesses.
            description: Optional metadata text.
            include_collector: If True, the monitor tracks its own memory too.
        """
        if self._worker_process and self._worker_process.is_alive():
            return  # idempotency

        main_collector, children_collector = self._check_metric_collector(
            main=main_metric_collector, children=children_metric_collector
        )

        self._event = self._ctx.Event()
        main_pid = os.getpid()
        log_path = reserve_path(log_dir=log_dir, filename=file_name, mode=file_mode)

        meta = MemoryMetadata(
            title=title or "unnamed-session",
            main_interval=main_interval,
            children_interval=children_interval,
            main_collector=main_collector,
            children_collector=children_collector,
            pid=main_pid,
            include_children=include_children,
            description=description,
        )

        # spawn process
        self._worker_process = self._ctx.Process(
            target=_monitor_worker,
            args=(
                self._event,
                main_pid,
                log_path,
                meta.to_json(),
                main_interval,
                children_interval,
                main_collector,
                children_collector,
                include_children,
                include_collector,
            ),
            daemon=True,
            name=f"ram-metric-collector-{main_pid}",
        )
        self._worker_process.start()

    @staticmethod
    def _check_metric_collector(
        main: str, children: str | None = None
    ) -> tuple[str, str]:
        if main not in _VALID_COLLECTORS:
            raise ValueError(
                f"Main process RAM metric collector can't be: [{main}]. Available: {_VALID_COLLECTORS}"
            )

        children = children or main

        if children not in _VALID_COLLECTORS:
            raise ValueError(
                f"Invalid children collector: [{children}]. Available: {_VALID_COLLECTORS}"
            )

        return main, children

    def stop(self) -> None:
        if self._event:
            self._event.set()

        if self._worker_process and self._worker_process.is_alive():
            self._worker_process.join(timeout=2.0)
            if self._worker_process.is_alive():
                self._worker_process.terminate()  # Force kill if it's hanging
        self._event = None
        self._worker_process = None

    @contextmanager
    def session(
        self,
        title: str | None = None,
        main_interval: float = 0.2,
        children_interval: float = 0.5,
        main_metric_collector: MemoryCollectorType = "pss",
        log_dir: str = "logs",
        file_name: str = "ram_sessions.log",
        include_children: bool = True,
        file_mode: FileMode = "append",
        children_metric_collector: MemoryCollectorType | None = None,
        description: str | None = None,
        include_collector: bool = False,
    ) -> Generator[None, None, None]:
        """Context manager that wraps start() and stop().

        Args:
            title: Session name for the log header.
            main_interval: How often (seconds) to sample the main process.
            children_interval: How often (seconds) to sample child processes.
            main_metric_collector: Metric type for the main process.
            children_metric_collector: Metric type for children (defaults to main_metric_collector).
            log_dir: Folder where logs will be saved.
            file_name: Name of the log file.
            file_mode: 'append' to keep old logs, 'write' to overwrite.
            include_children: If True, tracks all subprocesses.
            description: Optional metadata text.
            include_collector: If True, the monitor tracks its own memory too.
        """
        try:
            self.start(
                title=title,
                main_interval=main_interval,
                children_interval=children_interval,
                main_metric_collector=main_metric_collector,
                log_dir=log_dir,
                file_name=file_name,
                include_children=include_children,
                file_mode=file_mode,
                children_metric_collector=children_metric_collector,
                description=description,
                include_collector=include_collector,
            )
            yield
        except Exception as e:
            print(f"Error during MemoryCollector session: {e}")
            raise e
        finally:
            self.stop()


def _monitor_worker(
    stop_event: Event,
    target_pid: int,
    log_path: str,
    meta_json: str,
    main_interval: float,
    children_interval: float,
    main_collector: str,
    children_collector: str,
    include_children: bool,
    include_collector: bool,
    sep: str = MEMORY_SEP,
):
    """
    A separate process. Handles its own logging and polling.
    """
    logger = logging.getLogger(f"ram-metric-collector-{target_pid}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()

    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)

    # Log the header once
    logger.info(f"{MEMORY_HEADER}{sep}{meta_json}")

    try:
        main_proc = psutil.Process(target_pid)
    except psutil.NoSuchProcess:
        return  # Target already dead

    collector_pid = os.getpid()
    t0 = time.perf_counter()

    # Track next execution times to handle different intervals in one loop
    main_next_tick = t0
    child_next_tick = t0

    # readers
    _main_reader = _MEMORY_READERS[main_collector]
    _child_reader = _MEMORY_READERS[children_collector]

    while not stop_event.is_set():
        now = time.perf_counter()

        # # Check if target process died
        # if not main_proc.is_running():
        #     break

        if now >= main_next_tick:
            try:
                mem_usage = _main_reader(main_proc)
                msg = sep.join(
                    [
                        str(MemoryRole.MAIN.value),
                        f"{now - t0:.4f}",
                        str(format_mb(mem_usage)),
                    ]
                )
                logger.info(msg)

                # If collector block for too long that we missed ticks,
                # our loop tries to catch up so it logs burst of small-interval logs
                main_next_tick += main_interval
                now_after_poll = time.perf_counter()
                if main_next_tick < now_after_poll:
                    main_next_tick = now_after_poll + main_interval
            except psutil.NoSuchProcess:
                break  # Exit loop if main process dies

        # children
        if include_children and now >= child_next_tick:
            try:
                children = main_proc.children(recursive=True)
                if not include_collector:
                    children = [c for c in children if c.pid != collector_pid]

                total_mem = 0
                for child in children:
                    try:
                        total_mem += _child_reader(child)
                    except psutil.NoSuchProcess:
                        continue

                msg = sep.join(
                    [
                        str(MemoryRole.CHILD.value),
                        f"{now - t0:.4f}",
                        str(format_mb(total_mem)),
                        str(len(children)),
                    ]
                )
                logger.info(msg)
                child_next_tick += children_interval
                now_after_poll = time.perf_counter()
                if child_next_tick < now_after_poll:
                    child_next_tick = now_after_poll + children_interval
            except psutil.NoSuchProcess:
                break

        # Sleep until the next required tick, but wake up periodically to check stop_event
        if include_children:
            next_tick = min(main_next_tick, child_next_tick)
        else:
            next_tick = main_next_tick

        sleep_time = max(0.01, next_tick - time.perf_counter())
        stop_event.wait(timeout=sleep_time)

    # Cleanup
    for h in logger.handlers:
        h.flush()
        h.close()
