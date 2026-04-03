import time


def add_task(a: int, b: int) -> int:
    return a + b


def fast_task(x: int) -> int:
    return x * 2


def slow_task(duration: float) -> str:
    time.sleep(duration)
    return "done"


def conditional_slow_task(event, duration: float):
    event.wait(3)
    slow_task(duration)


def failing_task(msg: str = "Task failed!") -> None:
    raise ValueError(msg)
