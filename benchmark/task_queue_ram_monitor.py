import multiprocessing as mp  # noqa
import sys  # noqa
import time  # noqa
from typing import Literal

from tadween_core.devtools.analytics.collectors.ram import MemoryMonitor  # noqa
from tadween_core.task_queue import init_queue  # noqa

WAIT = 0.5

workers = 1
worker_type: Literal["thread", "process"] = "thread"
start_method: Literal["fork", "spawn"] = "spawn"
retain_result: bool = False


def heavy_result_fn(size: float = 10):
    time.sleep(0.2)
    return bytearray(size * 1024 * 1024)


def main():
    task_queue = init_queue(worker_type, max_workers=workers)
    ids = []
    for _i in range(10):
        ids.append(
            task_queue.submit(heavy_result_fn, size=10, retain_result=retain_result)
        )

    task_queue.wait_all()
    time.sleep(0.2)


if __name__ == "__main__":
    mp.set_start_method(start_method)
    worker_tag = (
        f"{worker_type[0]}{workers}-{start_method}"
        if worker_type == "process"
        else f"{worker_type[0]}{workers}"
    )
    title = f"{worker_tag}{'-retain' if retain_result else ''}"
    MemoryMonitor.start(
        interval=0.1,
        log_dir="/home/projects/tadween/core/benchmark/results",
        file_name="RAM_100mb.log",
        title=title,
        metric_collector="pss",
        file_mode="append",
    )
    main()
    time.sleep(WAIT)
