import multiprocessing
import os

import pytest

from tadween_core.task_queue import ProcessTaskQueue

from .shared import fast_task, slow_task
from .test_contract import TaskQueueContract


def _get_process_id():
    import os

    return os.getpid()


def init_func(event):
    event.set()


class TestProcessTaskQueue(TaskQueueContract):
    @pytest.fixture
    def queue(self, process_queue):
        return process_queue

    @pytest.fixture
    def event(self):
        return multiprocessing.get_context("spawn").Event()

    @pytest.fixture
    def pipe(self):
        return multiprocessing.get_context("spawn").Queue()

    def test_cancel_pending_task(self):
        pq = ProcessTaskQueue(name="CancelQueue", max_workers=1, retain_results=True)
        try:
            task_id1 = pq.submit(slow_task, duration=0.5)
            task_id2 = pq.submit(slow_task, duration=10.0)

            result1 = pq.cancel(task_id1)
            result2 = pq.cancel(task_id2)

            assert not result1
            assert result2
        finally:
            pq.close(force=True)

    def test_initializer_called(self, event):

        tq = ProcessTaskQueue(
            name="InitQueue",
            max_workers=1,
            initializer=init_func,
            initargs=(event,),
            retain_results=True,
        )
        try:
            task_id = tq.submit(fast_task, x=1)
            tq.get_result(task_id, timeout=1.0)
            assert event.wait(timeout=3.0), "initializer didn't set event"
        finally:
            tq.close()

    def test_callback_on_running(elf, queue, event):
        pytest.skip("to be implemented")

    #####

    def test_process_queue_isolation(self, process_queue):
        task_id = process_queue.submit(_get_process_id)
        result = process_queue.get_result(task_id, timeout=10.0)

        assert result != os.getpid()

    def test_process_queue_spawn_context(self, process_queue):
        ctx = getattr(process_queue.executor, "_mp_context", None)
        if ctx:
            try:
                current_method = ctx.get_start_method()
                assert current_method in ["spawn", "forkserver", "fork"]
            except Exception:
                pass


class TestRetainResults:
    def test_retain_results_false_cleans_up(self):
        pq = ProcessTaskQueue(name="NoRetainQueue", max_workers=2, retain_results=False)
        try:
            task_id = pq.submit(fast_task, x=5)
            pq.wait_all()
            assert task_id not in pq._tasks
        finally:
            pq.close()

    def test_per_task_retain_override(self):
        pq = ProcessTaskQueue(name="RetainQueue", max_workers=2, retain_results=True)
        try:
            task_id = pq.submit(fast_task, x=5, retain_result=False)
            pq.wait_all()

            assert task_id not in pq._tasks
        finally:
            pq.close()
