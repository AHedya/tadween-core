import time

import pytest

from tadween_core.task_queue.thread_queue import ThreadTaskQueue


def simple_task(x: int) -> int:
    return x * 2


def slow_task(duration: float) -> str:
    time.sleep(duration)
    return "done"


def failing_task() -> None:
    raise ValueError("Task failed!")


class TestThreadTaskQueueBasic:
    def test_submit_and_get_result(self, thread_queue):
        task_id = thread_queue.submit(simple_task, x=10)
        result = thread_queue.get_result(task_id, timeout=5.0)

        assert result == 20

    def test_submit_multiple_tasks(self):
        tq = ThreadTaskQueue(name="MultiQueue", max_workers=4, retain_results=True)
        try:
            task_ids = [tq.submit(simple_task, x=i) for i in range(5)]
            results = [tq.get_result(tid, timeout=5.0) for tid in task_ids]
            assert results == [0, 2, 4, 6, 8]
        finally:
            tq.close()

    def test_submit_with_multiple_args(self, thread_queue):
        def add(a: int, b: int) -> int:
            return a + b

        task_id = thread_queue.submit(add, a=5, b=3)
        result = thread_queue.get_result(task_id, timeout=5.0)

        assert result == 8


class TestThreadTaskQueueCallbacks:
    def test_on_submit_callback(self, thread_queue):
        submitted = []

        def on_submit_cb(task_id: str) -> None:
            submitted.append(task_id)

        task_id = thread_queue.submit(simple_task, x=5, on_submit=on_submit_cb)
        thread_queue.get_result(task_id, timeout=5.0)

        assert len(submitted) == 1
        assert submitted[0] == task_id

    def test_on_done_callback(self, thread_queue):
        completed = []

        def on_done_cb(task_id: str, future) -> None:  # noqa: ARG001
            completed.append(task_id)

        task_id = thread_queue.submit(simple_task, x=3, on_done=on_done_cb)
        thread_queue.get_result(task_id, timeout=5.0)

        assert len(completed) == 1

    def test_on_done_receives_successful_result(self, thread_queue):
        results = []

        def on_done_cb(task_id: str, future) -> None:  # noqa: ARG001
            envelope = future.result()
            if envelope.success:
                results.append(envelope.payload)

        task_id = thread_queue.submit(simple_task, x=7, on_done=on_done_cb)
        thread_queue.get_result(task_id, timeout=5.0)

        assert results == [14]

    def test_on_running_callback(self, thread_queue):
        running = []

        def on_running_cb(task_id: str) -> None:
            running.append(task_id)

        task_id = thread_queue.submit(simple_task, x=2, on_running=on_running_cb)
        thread_queue.get_result(task_id, timeout=5.0)

        assert len(running) == 1


class TestThreadTaskQueueErrorHandling:
    def test_failing_task_propagates_error(self, thread_queue):
        task_id = thread_queue.submit(failing_task)

        with pytest.raises(ValueError, match="Task failed!"):
            thread_queue.get_result(task_id, timeout=5.0)

    def test_on_done_receives_failed_result(self, thread_queue):
        errors = []

        def on_done_cb(task_id: str, future) -> None:  # noqa: ARG001
            envelope = future.result()
            if not envelope.success:
                errors.append(envelope.error)

        task_id = thread_queue.submit(failing_task, on_done=on_done_cb)

        try:
            thread_queue.get_result(task_id, timeout=5.0)
        except ValueError:
            pass

        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)


class TestThreadTaskQueueLifecycle:
    def test_close_waits_for_tasks(self):
        tq = ThreadTaskQueue(name="CloseQueue", max_workers=2)
        try:
            tq.submit(slow_task, duration=0.2)
            tq.close()
            assert tq._closed is True
        finally:
            if not tq._closed:
                tq.close()

    def test_submit_after_close_raises(self):
        tq = ThreadTaskQueue(name="ClosedQueue", max_workers=2)
        try:
            tq.close()
            with pytest.raises(RuntimeError, match="is closed"):
                tq.submit(simple_task, x=1)
        finally:
            if not tq._closed:
                tq.close()


class TestThreadTaskQueueRetainResults:
    def test_retain_results_false_cleans_up(self):
        tq = ThreadTaskQueue(name="NoRetainQueue", max_workers=2, retain_results=False)
        try:
            task_id = tq.submit(simple_task, x=5)
            time.sleep(0.2)
            assert task_id not in tq._tasks
        finally:
            tq.close()

    def test_per_task_retain_override(self):
        tq = ThreadTaskQueue(name="RetainQueue", max_workers=2, retain_results=True)
        try:
            task_id = tq.submit(simple_task, x=5, retain_result=False)
            time.sleep(0.2)
            assert task_id not in tq._tasks
        finally:
            tq.close()


class TestThreadTaskQueueInitializer:
    def test_initializer_called(self):
        initialized = []

        def init_func() -> None:
            initialized.append(True)

        tq = ThreadTaskQueue(
            name="InitQueue", max_workers=1, initializer=init_func, retain_results=True
        )
        try:
            task_id = tq.submit(simple_task, x=1)
            tq.get_result(task_id, timeout=5.0)
            assert len(initialized) == 1
        finally:
            tq.close()
