import logging
import multiprocessing as mp
import os  # noqa
import time  # noqa
from pathlib import Path
from typing import Literal

from tadween_core.broker import InMemoryBroker, Message  # noqa
from tadween_core.cache import get_cache  # noqa
from tadween_core.devtools.analytics.collectors.ram import (  # noqa
    FileMode,
    MemoryMonitor,
)
from tadween_core.stage import Stage  # noqa
from tadween_core.task_queue import (
    init_queue,  # noqa
)

FILE_DIR = Path(__file__).parent
logger = logging.getLogger(__name__)


def heavy_result_fn(size_mb):
    size = max(size_mb * 1048576, 1048576)
    dumb_data = bytearray(size)
    time.sleep(0.2)
    # for i in range(10_000_000):
    #     x = (i * 2) / 3 + 10  # noqa: F841
    return dumb_data


# METRIC_COLLECTORS = ("pss", "rss", "uss")
METRIC_COLLECTORS = ("uss",)

EXECUTOR_TYPES = ("thread", "process")
WORKER_COUNTS = (1, 2, 5)
BATCH_SIZES = (10,)


def run_test(
    interval: float,
    msg: str,
    log_dir: str,
    file_name: str,
    metric_collector: Literal["rss", "pss"],
    file_mode: FileMode,
    executor_type: Literal["thread", "process"],
    max_workers: int,
    batch_size: int,
):
    MemoryMonitor.start(
        interval=interval,
        title=msg,
        log_dir=log_dir,
        file_name=file_name,
        metric_collector=metric_collector,
        file_mode=file_mode,
    )
    try:
        task_queue = init_queue(
            executor_type,
            max_workers=max_workers,
        )

        for _ in range(batch_size):
            _ = task_queue.submit(heavy_result_fn, size_mb=10)

        task_queue.close()
    except Exception as e:
        print(f"Error happen: {e}")
    finally:
        MemoryMonitor.stop()
        print(f"{msg} Done")


if __name__ == "__main__":
    mp.set_start_method("fork")
    for collector in METRIC_COLLECTORS:
        for executor in EXECUTOR_TYPES:
            for n_workers in WORKER_COUNTS:
                for bs in BATCH_SIZES:
                    msg = f"{executor[0]}{n_workers}-{collector}"
                    run_test(
                        0.1,
                        msg=msg,
                        log_dir=FILE_DIR / "results",
                        file_name="100mb-pre-opt-fork.log",
                        file_mode="append",
                        metric_collector=collector,
                        executor_type=executor,
                        max_workers=n_workers,
                        batch_size=bs,
                    )


# bs = 10
# collector = "pss"
# executor = "process"
# n_workers = 5  # 2,5

# msg = f"{executor[0]}{n_workers}-{collector}"
# run_test(
#     0.1,
#     msg=msg,
#     log_dir=FILE_DIR / "results",
#     file_name="100mb.log",
#     file_mode="append",
#     metric_collector=collector,
#     executor_type=executor,
#     max_workers=n_workers,
#     batch_size=bs,
# )
