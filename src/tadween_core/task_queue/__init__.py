from collections.abc import Callable
from logging import Logger
from typing import Literal, TypeAlias, get_args, overload

from .base_queue import BaseTaskPolicy, BaseTaskQueue
from .dynamic import DynamicTaskQueue, ExecutorType
from .process_queue import ProcessTaskQueue
from .thread_queue import ThreadTaskQueue

QueueExecutorType: TypeAlias = Literal["thread", "process", "dynamic"]  # noqa: UP040
valid = ", ".join(get_args(QueueExecutorType))


@overload
def init_queue(
    executor: Literal["thread"],
    *,
    name: str | None = None,
    max_workers: int | None = None,
    default_policy: type[BaseTaskPolicy] | None = None,
    logger: Logger | None = None,
    initializer: Callable | None = None,
    initargs: tuple = (),
) -> ThreadTaskQueue: ...


@overload
def init_queue(
    executor: Literal["process"],
    *,
    name: str | None = None,
    max_workers: int | None = None,
    default_policy: type[BaseTaskPolicy] | None = None,
    logger: Logger | None = None,
    initializer: Callable | None = None,
    initargs: tuple = (),
) -> ProcessTaskQueue: ...


# @overload
# def init_queue(
#     executor: Literal["dynamic"],
#     *,
#     name: str | None = None,
#     default_executor: Literal["thread", "process"] | None = None,
#     max_thread_workers: int | None = None,
#     max_process_workers: int | None = None,
#     default_policy: type[BaseTaskPolicy] | None = None,
#     logger: Logger | None = None,
#     initializer: Callable | None = None,
#     initargs: tuple = (),
# ) -> DynamicTaskQueue: ...


@overload
def init_queue(
    executor: Literal["thread"] = "thread",
    *,
    name: str | None = None,
    max_workers: int | None = None,
    default_policy: type[BaseTaskPolicy] | None = None,
    logger: Logger | None = None,
    initializer: Callable | None = None,
    initargs: tuple = (),
) -> ThreadTaskQueue: ...


def init_queue(
    executor: QueueExecutorType = "thread",
    *,
    name: str | None = None,
    max_workers: int | None = None,
    default_executor: Literal["thread", "process"] | None = None,
    default_policy: type[BaseTaskPolicy] | None = None,
    logger: Logger | None = None,
    max_thread_workers: int | None = None,
    max_process_workers: int | None = None,
    initializer: Callable | None = None,
    initargs: tuple = (),
) -> BaseTaskQueue:
    if executor == "dynamic":
        return DynamicTaskQueue(
            name=name,
            # map from str to enum
            default_executor=ExecutorType(default_executor.upper())
            if default_executor
            else None,
            max_thread_workers=max_thread_workers,
            max_process_workers=max_process_workers,
        )
    elif executor == "process":
        return ProcessTaskQueue(
            name=name,
            max_workers=max_workers,
            default_policy=default_policy,
            logger=logger,
            initializer=initializer,
            initargs=initargs,
        )
    elif executor == "thread":
        return ThreadTaskQueue(
            name=name,
            max_workers=max_workers,
            default_policy=default_policy,
            logger=logger,
            initializer=initializer,
            initargs=initargs,
        )
    else:
        raise ValueError(
            f"Task queue type executor can only be [{get_args(QueueExecutorType)}]. Got: {executor}"
        )


__all__ = [
    "init_queue",
    "BaseTaskQueue",
    "ThreadTaskQueue",
    "ProcessTaskQueue",
    "DynamicTaskQueue",
    "ExecutorType",
]
