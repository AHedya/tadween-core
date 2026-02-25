import logging
import os
import threading
import time
from logging.handlers import RotatingFileHandler
from typing import Literal, TypeAlias

import psutil

from ...utils import FileMode, reserve_path
from ..schemas import (
    RAM_HEADER,
    RAM_SEP,
    RAMMetadata,
)


class MemoryMetricCollector:
    """Base class for different memory metric collection strategies"""

    def collect_process_memory(self, process: psutil.Process) -> int:
        """Collect memory metric for a process in bytes"""
        raise NotImplementedError

    def metric_name(self) -> str:
        """Return the name of the metric (e.g., 'RSS', 'PSS')"""
        raise NotImplementedError


class RSSCollector(MemoryMetricCollector):
    """Collects Resident Set Size - fast, includes shared memory"""

    @staticmethod
    def collect_process_memory(process: psutil.Process) -> int:
        return process.memory_info().rss

    @staticmethod
    def metric_name() -> str:
        return "RSS"


class USSCollector(MemoryMetricCollector):
    """Collects Unique Set."""

    @staticmethod
    def collect_process_memory(process: psutil.Process) -> int:
        return process.memory_full_info().uss

    @staticmethod
    def metric_name() -> str:
        return "USS"


class PSSCollector(MemoryMetricCollector):
    """Collects Unique Set."""

    @staticmethod
    def collect_process_memory(process: psutil.Process) -> int:
        return process.memory_full_info().pss

    @staticmethod
    def metric_name() -> str:
        return "PSS"


RAMCollector: TypeAlias = Literal["rss", "pss", "uss"]


class MemoryMonitor:
    _thread: threading.Thread | None = None
    _discovery_thread: threading.Thread | None = None
    _logger: logging.Logger | None = None
    _log_path: str | None = None

    _stop_event = threading.Event()

    # Children cache
    _children_cache = {"total_memory": 0, "count": 0}
    _cache_lock = threading.Lock()

    # Metric collector
    _metric_collector: MemoryMetricCollector = RSSCollector()

    @staticmethod
    def _format_mb(n_bytes):
        return round(n_bytes / 1024 / 1024, 2)

    @classmethod
    def _setup_logging(cls, log_dir: str, filename: str, file_mode: str | None = None):
        """
        Configure the logger and rotating file handler. Will not overwrite an existing
        file unless overwrite=True. If called multiple times with different target files
        the old handlers are closed and replaced.
        """
        # Reserve a file path (atomic where possible)
        path = reserve_path(log_dir, filename, file_mode)

        # If an existing logger already points to the same file, nothing to do.
        if cls._logger:
            for h in cls._logger.handlers:
                try:
                    if getattr(h, "baseFilename", None) == path:
                        cls._log_path = path
                        return
                except Exception:
                    pass

            for h in list(cls._logger.handlers):
                try:
                    cls._logger.removeHandler(h)
                    h.close()
                except Exception:
                    pass

        # Create a distinct logger name per logfile to make multiple monitors co-exist
        logger_name = f"MemoryMonitor:{os.path.basename(path)}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # Minimal message format (timestamp handling can be done by monitor message)
        formatter = logging.Formatter("%(message)s")
        handler = RotatingFileHandler(
            path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        cls._logger = logger
        cls._log_path = path

    @classmethod
    def start(
        cls,
        interval: float = 0.5,
        title: str | None = None,
        description: str | None = None,
        log_dir: str = "logs",
        file_name: str = "ram_sessions.log",
        include_children: bool = True,
        file_mode: FileMode = "append",
        metric_collector: RAMCollector = "pss",
    ):
        """
        Start memory monitoring.
        """
        if cls._thread and cls._thread.is_alive():
            return

        cls._setup_logging(log_dir, file_name, file_mode)
        # Reset stop event
        cls._stop_event.clear()

        # Set up metric collector
        if metric_collector == "pss":
            cls._metric_collector = PSSCollector
        elif metric_collector == "rss":
            cls._metric_collector = RSSCollector
        elif metric_collector == "uss":
            cls._metric_collector = USSCollector
        else:
            raise ValueError(f"Not recognizable collector: {metric_collector}")

        # Start child process discovery thread if needed
        if include_children and (
            not cls._discovery_thread or not cls._discovery_thread.is_alive()
        ):

            def _discovery_worker():
                this_proc = psutil.Process(os.getpid())
                while not cls._stop_event.is_set():
                    try:
                        children = this_proc.children(recursive=True)
                        total_memory = sum(
                            cls._metric_collector.collect_process_memory(c)
                            for c in children
                        )
                        with cls._cache_lock:
                            cls._children_cache = {
                                "total_memory": total_memory,
                                "count": len(children),
                            }
                    except Exception:
                        # Log error but continue
                        with cls._cache_lock:
                            cls._children_cache = {
                                "total_memory": 0,
                                "count": 0,
                            }
                    time.sleep(1.0)

            # FIX: Create thread first, then start it
            cls._discovery_thread = threading.Thread(
                target=_discovery_worker, daemon=True
            )
            cls._discovery_thread.start()

        def _worker():
            current_proc = psutil.Process(os.getpid())
            start = time.perf_counter()

            metric_name = cls._metric_collector.metric_name()
            session_header = RAMMetadata(
                title=title,
                description=description,
                pid=current_proc.pid,
                sampling_interval=interval,
                children_included=include_children,
                metric_collector=metric_name,
            )
            header = f"{RAM_HEADER}{RAM_SEP}{session_header.to_json()}"
            cls._logger.info(header)

            while not cls._stop_event.is_set():
                try:
                    main_memory = cls._metric_collector.collect_process_memory(
                        current_proc
                    )
                    with cls._cache_lock:
                        child_memory = cls._children_cache["total_memory"]
                        child_count = cls._children_cache["count"]
                    total_mb = cls._format_mb(main_memory + child_memory)
                    main_mb = cls._format_mb(main_memory)
                    child_mb = cls._format_mb(child_memory)
                    elapsed = time.perf_counter() - start
                    # Data Line: ELAPSED_TIME|TOTAL_MB|MAIN_MB|CHILD_MB|CHILD_COUNT
                    # We might validate/standardize against RAMSample model.
                    # Thus we have a centralized format.
                    # cls._logger.info(RAMSample(...).to_str())
                    cls._logger.info(
                        f"{elapsed:.3f}{RAM_SEP}{total_mb}{RAM_SEP}"
                        f"{main_mb}{RAM_SEP}{child_mb}{RAM_SEP}{child_count}"
                    )
                except Exception as e:
                    cls._logger.info(
                        f"ERROR|{time.perf_counter() - start:.3f}|{str(e)}"
                    )
                time.sleep(interval)

        cls._thread = threading.Thread(target=_worker, daemon=True)
        cls._thread.start()

    @classmethod
    def stop(cls):
        """
        Stop memory monitoring gracefully and log END marker.
        """
        if cls._thread and cls._thread.is_alive():
            cls._stop_event.set()
            cls._thread.join(timeout=2.0)

        if cls._discovery_thread and cls._discovery_thread.is_alive():
            # Discovery thread will stop when stop_event is set
            cls._discovery_thread.join(timeout=2.0)

        cls._thread = None
        cls._discovery_thread = None

    @classmethod
    def get_log_path(cls) -> str | None:
        """Return the path to the current log file, if any"""
        return cls._log_path

    @classmethod
    def is_running(cls) -> bool:
        """Check if monitoring is currently active"""
        return cls._thread is not None and cls._thread.is_alive()
