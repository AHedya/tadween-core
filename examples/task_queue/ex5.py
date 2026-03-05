import time

from tadween_core.task_queue import init_queue


def worker_init():
    import logging
    import sys
    from multiprocessing import current_process

    global logger
    process = current_process()
    logger = logging.getLogger(f"worker-{process.name}")
    logger.propagate = False
    logger.setLevel(logging.INFO)

    for h in logger.handlers:
        logger.removeHandler(h)
        h.close()

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.info("Worker setup complete")


def cpu_bound_task(n_elements):
    global logger

    begin = time.perf_counter()
    data = list(range(n_elements))
    res = 0
    for d in data:
        res += d * d
    end = time.perf_counter()
    logger.info(f"Task took: {end - begin:.4f}")
    return res


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.set_start_method("spawn")
    q = init_queue(
        "process",
        max_workers=4,
        initializer=worker_init,
    )

    begin = time.perf_counter()
    for _ in range(1, 10):
        q.submit(fn=cpu_bound_task, n_elements=20_000_000)
    end = time.perf_counter()

    # Few milliseconds. Async execution
    print(f"All tasks submitted after: {end - begin:.3f}")
