import threading
import time
from queue import Queue

import pytest

from tadween_core.task_queue.thread_queue import ThreadTaskQueue

from .shared import conditional_slow_task
from .test_contract import TaskQueueContract, fast_task, slow_task


class TestThreadTaskQueue(TaskQueueContract):
    @pytest.fixture
    def queue(self, thread_queue):
        return thread_queue

    @pytest.fixture
    def event(self):
        return threading.Event()

    @pytest.fixture
    def pipe(self):
        return Queue()

    def test_cancel_pending_task(self, event):
        tq = ThreadTaskQueue(name="CancelQueue", max_workers=1, retain_results=True)
        try:
            task_id1 = tq.submit(conditional_slow_task, event=event, duration=0.2)
            task_id2 = tq.submit(slow_task, duration=5.0)

            result1 = tq.cancel(task_id1)
            result2 = tq.cancel(task_id2)
            event.set()

            assert not result1
            assert result2
        finally:
            tq.close(force=False)

    def test_initializer_called(self, event):

        def init_func(event) -> None:
            event.set()

        tq = ThreadTaskQueue(
            name="InitQueue",
            max_workers=1,
            initializer=init_func,
            initargs=(event,),
            retain_results=True,
        )
        try:
            task_id = tq.submit(fast_task, x=1)
            tq.get_result(task_id, timeout=5.0)
            event.wait(timeout=5.0)
        finally:
            tq.close()


class TestRetainResults:
    def test_retain_results_false_cleans_up(self):
        tq = ThreadTaskQueue(name="NoRetainQueue", max_workers=2, retain_results=False)
        try:
            task_id = tq.submit(fast_task, x=5)
            time.sleep(0.2)
            assert task_id not in tq._tasks
        finally:
            tq.close()

    def test_per_task_retain_override(self):
        tq = ThreadTaskQueue(name="RetainQueue", max_workers=2, retain_results=True)
        try:
            task_id = tq.submit(fast_task, x=5, retain_result=False)
            time.sleep(0.2)
            assert task_id not in tq._tasks
        finally:
            tq.close()
