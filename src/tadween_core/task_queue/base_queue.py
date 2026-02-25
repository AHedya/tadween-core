import logging
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from typing import Annotated, Any, Generic

from tadween_core.task_queue.base_policy import BaseTaskPolicy
from tadween_core.task_queue.policy import NoOpPolicy

from .base import T, TaskEnvelope, TaskMetadata, TaskStatus


# module level. child processes can't find closures
def _execute_wrapped(
    on_running: Callable[[str], None],
    task_id: str,
    fn: Callable,
    kwargs: dict[str, Any],
    submit_time: float,
) -> TaskEnvelope:
    on_running(task_id)
    start_t = time.perf_counter()
    try:
        result = fn(**kwargs)
        end_t = time.perf_counter()
        return TaskEnvelope(
            payload=result,
            metadata=TaskMetadata(task_id, start_t, end_t, submit_time),
            success=True,
        )
    except Exception as e:
        end_t = time.perf_counter()
        return TaskEnvelope(
            payload=None,
            metadata=TaskMetadata(task_id, start_t, end_t, submit_time),
            error=e,
            success=False,
        )


class BaseTaskQueue(ABC, Generic[T]):
    """Base interface for task queues"""

    def __init__(
        self,
        name: str | None = None,
        logger: logging.Logger | None = None,
        default_policy: type[BaseTaskPolicy] | None = None,
        retain_results: bool = True,
    ):
        self.name = name or f"{self.__class__.__name__}-{id(self):x}"
        self.logger = logger or logging.getLogger(f"tadween.{self.name}")
        self.retain_results = retain_results
        self.default_policy = default_policy or NoOpPolicy
        self._tasks: dict[str, Future[TaskEnvelope[T]]] = {}
        self._lock = threading.Lock()
        self._closed = False
        # Set by subclasses. Either process or thread executor
        self.executor: Executor | None = None

    def submit(
        self,
        fn: Callable,
        on_submit: Callable[[Annotated[str, "task_id"]], None] | None = None,
        on_running: Callable[[Annotated[str, "task_id"]], None] | None = None,
        on_done: Callable[[Annotated[str, "task_id"], Future[TaskEnvelope[T]]], None]
        | None = None,
        retain_result: bool | None = None,
        **kwargs,
    ) -> str:
        """Submit a task for execution. Returns a task ID."""
        on_submit = on_submit or self.default_policy.on_submit
        on_running = on_running or self.default_policy.on_running
        on_done = on_done or self.default_policy.on_done
        retain_result = self.retain_results if retain_result is None else retain_result

        if self._closed:
            raise RuntimeError(f"Task queue [{self.name}] is closed")

        task_id = str(uuid.uuid4())
        submit_time = time.perf_counter()
        on_submit(task_id)

        try:
            future = self.executor.submit(
                _execute_wrapped, on_running, task_id, fn, kwargs, submit_time
            )
            with self._lock:
                self._tasks[task_id] = future

            def _done_callback(future):
                on_done(task_id, future)
                if not retain_result:
                    self._tasks.pop(task_id, None)

            future.add_done_callback(_done_callback)
            return task_id
        except Exception as e:
            self.logger.error(f"Failed to submit task: {e}", exc_info=True)
            raise

    def get_status(self, task_id: str) -> TaskStatus:
        with self._lock:
            if task_id not in self._tasks:
                raise ValueError(f"Unknown task ID: {task_id}")
            future = self._tasks[task_id]
        if future.cancelled():
            return TaskStatus.CANCELLED
        if not future.done():
            return TaskStatus.RUNNING
        try:
            envelope: TaskEnvelope = future.result()
            return TaskStatus.COMPLETED if envelope.success else TaskStatus.FAILED
        except Exception:
            return TaskStatus.FAILED

    def get_all_statuses(self) -> dict[str, TaskStatus]:
        """
        Returns a snapshot of the status of all currently tracked tasks.
        """
        statuses = {}
        with self._lock:
            ids = list(self._tasks.keys())
        for task_id in ids:
            try:
                statuses[task_id] = self.get_status(task_id)
            except ValueError:
                continue
        return statuses

    def stream_completed(self, timeout: float | None = None):
        """
        Generator yielding (task_id, result) as tasks finish.
        Yields exceptions as results for failed tasks.
        """
        if not self.retain_results:
            self.logger.warning(
                "stream_completed() requires retain_results=True on either task, or task queue itself. "
                f"{self.name}.retain_result is set to False. Use on_done callbacks for non-retained results."
            )
        with self._lock:
            future_to_id = {fut: tid for tid, fut in self._tasks.items()}
            futures = list(future_to_id.keys())

        for future in as_completed(futures, timeout=timeout):
            task_id = future_to_id[future]
            try:
                envelope: TaskEnvelope = future.result()
                if envelope.success:
                    yield task_id, envelope.payload
                else:
                    yield task_id, envelope.error
            except Exception as system_error:
                yield task_id, system_error
            finally:
                with self._lock:
                    del self._tasks[task_id]

    def cancel(self, task_id: str) -> bool:
        """Attempt to cancel a task"""
        with self._lock:
            if task_id not in self._tasks:
                return False
            future = self._tasks[task_id]

        return future.cancel()

    def get_result(
        self,
        task_id: str,
        timeout: float | None = None,
        consume: bool = True,
    ) -> T:
        """Get result, blocking until available.
        Args:
            task_id (str): task id
            timeout (float | None, optional): timeout. Defaults to None.
            consume (bool, optional): automatically delete task and result after reading. Defaults to True.

        Raises:
            ValueError: Task isn't found
            envelope.error: Error during execution the task

        Returns:
            Any: Task result
        """
        with self._lock:
            if task_id not in self._tasks:
                raise ValueError(f"Unknown task ID: {task_id}")
            future = self._tasks[task_id]

        try:
            envelope: TaskEnvelope = future.result(timeout=timeout)
            if not envelope.success:
                raise envelope.error
            return envelope.payload
        finally:
            if consume:
                with self._lock:
                    del self._tasks[task_id]

    def wait_all(self, timeout: float | None = None):
        """Hangs the main process until all tasks in the queue are done."""
        with self._lock:
            pending = set(self._tasks.values())

        start_time = time.perf_counter()

        while pending:
            rem_timeout = None
            if timeout is not None:
                rem_timeout = timeout - (time.perf_counter() - start_time)
                if rem_timeout <= 0:
                    raise TimeoutError("Task queue timed out")

            done, pending = wait(
                pending, return_when=FIRST_COMPLETED, timeout=rem_timeout
            )
            done.clear()

    @abstractmethod
    def close(self) -> None:
        """Shutdown the task queue gracefully. ThreadPoolExecutor shutdown differs from ProcessPoolExecutor"""
        pass

    def __repr__(self):
        return f"<{self.name if self.name else 'ThreadPool' if type(self.executor) is ThreadPoolExecutor else 'ProcessPool'} workers={getattr(self.executor, '_max_workers', None)} closed={self._closed}>"
