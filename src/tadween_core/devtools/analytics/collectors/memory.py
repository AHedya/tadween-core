import logging  # noqa
import logging.handlers  # noqa
from contextlib import contextmanager
import multiprocessing as mp  # noqa
from multiprocessing.synchronize import Event
import os
import threading
import time
from typing import Literal, TypeAlias

import psutil
from tadween_core.devtools.utils import FileMode, reserve_path, format_mb

from ..schemas import (
    MEMORY_HEADER,
    MEMORY_SEP,
    MemoryMetadata,
    MemoryRole,
)

MemoryCollectorType: TypeAlias = Literal["rss", "pss", "uss"]
_VALID_COLLECTORS = ("rss", "pss", "uss")


class MemoryCollector:
    _ctx = mp.get_context("spawn")
    _queue = None
    _event = None
    _child_worker: mp.Process | None = None

    _listener: logging.handlers.QueueListener | None = None

    _main_worker: threading.Thread | None = None
    _main_logger: logging.Logger | None = None

    @classmethod
    def start(
        cls,
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
    ):
        # if running, return. Idempotency
        if cls._main_worker and cls._main_worker.is_alive():
            return

        # validate the inputs
        main_collector, children_collector = cls._check_metric_collector(
            main=main_metric_collector, children=children_metric_collector
        )

        # Never set on class level.
        cls._queue = cls._ctx.Queue()
        cls._event = cls._ctx.Event()

        # setup main logger
        main_pid = os.getpid()
        log_path = reserve_path(log_dir=log_dir, filename=file_name, mode=file_mode)
        cls._setup_logger(log_path)

        # Log Session Header
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
        cls._main_logger.info(f"{MEMORY_HEADER}{MEMORY_SEP}{meta.to_json()}")

        # run child worker
        if include_children:
            cls._child_worker = cls._ctx.Process(
                target=_child_worker,
                args=(
                    cls._queue,
                    cls._event,
                    children_interval,
                    main_pid,
                    children_collector,
                    include_collector,
                ),
                daemon=True,
                name="memory-collector-child",
            )
            cls._child_worker.start()

        # check and run main process worker
        cls._main_worker = threading.Thread(
            target=_worker,
            daemon=True,
            args=(
                cls._main_logger,
                cls._event,
                main_interval,
                main_collector,
            ),
            name="memory-collector-main",
        )
        cls._main_worker.start()

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
                f"Child processes RAM metric collector can't be: [{children}]. Available: {_VALID_COLLECTORS}"
            )

        return main, children

    @classmethod
    def _setup_logger(cls, log_path):
        cls._main_logger = logging.getLogger("RAM-metric")

        cls._main_logger.setLevel(logging.INFO)
        cls._main_logger.propagate = False

        # Cleanup existing handlers
        for h in list(cls._main_logger.handlers):
            cls._main_logger.removeHandler(h)
            h.close()

        # Setup Queue
        qh = logging.handlers.QueueHandler(cls._queue)
        cls._main_logger.addHandler(qh)

        fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(message)s"))

        # Setup Listener
        cls._listener = logging.handlers.QueueListener(cls._queue, fh)
        cls._listener.start()

    @classmethod
    def stop(cls) -> None:
        if cls._event:
            cls._event.set()

        if cls._main_worker and cls._main_worker.is_alive():
            cls._main_worker.join(timeout=2.0)

        if cls._child_worker and cls._child_worker.is_alive():
            cls._child_worker.join(timeout=2.0)
            # cls._child_worker.close() # close() can fail if process is still alive

        # Stop listener AFTER workers finish — ensures queue is fully drained
        if cls._listener:
            cls._listener.stop()
            cls._listener = None

        if cls._queue:
            cls._queue.close()
            cls._queue.join_thread()
            cls._queue = None

        # Also remove handler from main_logger to allow re-start with different file
        if cls._main_logger:
            for h in list(cls._main_logger.handlers):
                cls._main_logger.removeHandler(h)
                h.close()

        cls._event = None
        cls._main_worker = None
        cls._child_worker = None
        cls._main_logger = None

    @classmethod
    @contextmanager
    def session(
        cls,
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
    ):
        try:
            cls.start(
                title=title,
                main_interval=main_interval,
                children_interval=children_interval,
                main_metric_collector=main_metric_collector,
                children_metric_collector=children_metric_collector,
                log_dir=log_dir,
                file_mode=file_mode,
                file_name=file_name,
                include_children=include_children,
                description=description,
                include_collector=include_collector,
            )
            yield
        except Exception as e:
            print(f"Error during creating MemoryCollector session: {e}")
            raise e
        finally:
            cls.stop()


def _child_worker(
    queue: mp.Queue,
    event: Event,
    interval: float,
    main_pid: int,
    collector_type: MemoryCollectorType = "pss",
    include_self: bool = True,
    sep: str = MEMORY_SEP,
):
    _setup_child_logger(queue)
    import os
    import time

    import psutil

    logger = logging.getLogger("discovery_child_process_logger")
    pid = os.getpid()
    t0 = time.perf_counter()

    while not event.wait(timeout=interval):
        try:
            parent_proc = psutil.Process(main_pid)
            children = parent_proc.children(recursive=True)
            if not include_self:
                children = [c for c in children if c.pid != pid]

            total = 0
            for child in children:
                try:
                    total += getattr(child.memory_full_info(), collector_type)
                except psutil.NoSuchProcess:
                    pass  # process died between listing and querying

            msg = sep.join(
                [
                    str(MemoryRole.CHILD.value),
                    f"{time.perf_counter() - t0:.4f}",
                    str(format_mb(total)),
                    str(len(children)),
                ]
            )
            logger.info(msg)
        except psutil.NoSuchProcess:
            print("Main process not found, exiting child worker.")
            break
        except Exception as e:
            print(f"Error in children discovery worker: {e}.")


def _setup_child_logger(queue: mp.Queue):
    import logging
    import logging.handlers

    logger = logging.getLogger("discovery_child_process_logger")
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(logging.handlers.QueueHandler(queue))


def _worker(
    logger: logging.Logger,
    event: Event,
    interval: float,
    collector_type: str,
    sep: str = MEMORY_SEP,
):
    """Runs in a thread in the main process."""

    t0 = time.perf_counter()
    # while not event.is_set():
    while not event.wait(timeout=interval):
        try:
            current_proc = psutil.Process(os.getpid())
            mem_usage = getattr(current_proc.memory_full_info(), collector_type)
            msg = sep.join(
                [
                    str(MemoryRole.MAIN.value),
                    f"{time.perf_counter() - t0:.4f}",
                    str(format_mb(mem_usage)),
                ]
            )
            logger.info(msg)
        except Exception as e:
            print(f"Error in main thread memory monitor logger: {e}.")
        # time.sleep(interval)
