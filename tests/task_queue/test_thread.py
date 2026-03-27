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
    def test_submit_and_get_result(self):
        tq = ThreadTaskQueue(name="TestQueue", max_workers=2, retain_results=True)

        task_id = tq.submit(simple_task, x=10)
        result = tq.get_result(task_id, timeout=5.0)

        assert result == 20
        tq.close()

    def test_submit_multiple_tasks(self):
        tq = ThreadTaskQueue(name="MultiQueue", max_workers=4, retain_results=True)

        task_ids = [tq.submit(simple_task, x=i) for i in range(5)]

        results = [tq.get_result(tid, timeout=5.0) for tid in task_ids]

        assert results == [0, 2, 4, 6, 8]
        tq.close()

    def test_submit_with_multiple_args(self):
        def add(a: int, b: int) -> int:
            return a + b

        tq = ThreadTaskQueue(name="AddQueue", max_workers=2, retain_results=True)

        task_id = tq.submit(add, a=5, b=3)
        result = tq.get_result(task_id, timeout=5.0)

        assert result == 8
        tq.close()


class TestThreadTaskQueueCallbacks:
    def test_on_submit_callback(self):
        submitted = []

        def on_submit_cb(task_id: str) -> None:
            submitted.append(task_id)

        tq = ThreadTaskQueue(name="CallbackQueue", max_workers=2, retain_results=True)
        task_id = tq.submit(simple_task, x=5, on_submit=on_submit_cb)

        tq.get_result(task_id, timeout=5.0)

        assert len(submitted) == 1
        assert submitted[0] == task_id
        tq.close()

    def test_on_done_callback(self):
        completed = []

        def on_done_cb(task_id: str, future) -> None:  # noqa: ARG001
            completed.append(task_id)

        tq = ThreadTaskQueue(name="DoneQueue", max_workers=2, retain_results=True)
        task_id = tq.submit(simple_task, x=3, on_done=on_done_cb)

        tq.get_result(task_id, timeout=5.0)

        assert len(completed) == 1
        tq.close()

    def test_on_done_receives_successful_result(self):
        results = []

        def on_done_cb(task_id: str, future) -> None:  # noqa: ARG001
            envelope = future.result()
            if envelope.success:
                results.append(envelope.payload)

        tq = ThreadTaskQueue(name="ResultQueue", max_workers=2, retain_results=True)
        task_id = tq.submit(simple_task, x=7, on_done=on_done_cb)

        tq.get_result(task_id, timeout=5.0)

        assert results == [14]
        tq.close()

    def test_on_running_callback(self):
        running = []

        def on_running_cb(task_id: str) -> None:
            running.append(task_id)

        tq = ThreadTaskQueue(name="RunningQueue", max_workers=2, retain_results=True)
        task_id = tq.submit(simple_task, x=2, on_running=on_running_cb)

        tq.get_result(task_id, timeout=5.0)

        assert len(running) == 1
        tq.close()


class TestThreadTaskQueueErrorHandling:
    def test_failing_task_propagates_error(self):
        tq = ThreadTaskQueue(name="ErrorQueue", max_workers=2, retain_results=True)

        task_id = tq.submit(failing_task)

        with pytest.raises(ValueError, match="Task failed!"):
            tq.get_result(task_id, timeout=5.0)

        tq.close()

    def test_on_done_receives_failed_result(self):
        errors = []

        def on_done_cb(task_id: str, future) -> None:  # noqa: ARG001
            envelope = future.result()
            if not envelope.success:
                errors.append(envelope.error)

        tq = ThreadTaskQueue(name="ErrorDoneQueue", max_workers=2, retain_results=True)
        task_id = tq.submit(failing_task, on_done=on_done_cb)

        try:
            tq.get_result(task_id, timeout=5.0)
        except ValueError:
            pass

        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
        tq.close()


class TestThreadTaskQueueLifecycle:
    def test_close_waits_for_tasks(self):
        tq = ThreadTaskQueue(name="CloseQueue", max_workers=2)

        tq.submit(slow_task, duration=0.2)

        tq.close()

        assert tq._closed is True

    def test_submit_after_close_raises(self):
        tq = ThreadTaskQueue(name="ClosedQueue", max_workers=2)
        tq.close()

        with pytest.raises(RuntimeError, match="is closed"):
            tq.submit(simple_task, x=1)


class TestThreadTaskQueueRetainResults:
    def test_retain_results_false_cleans_up(self):
        tq = ThreadTaskQueue(name="NoRetainQueue", max_workers=2, retain_results=False)

        task_id = tq.submit(simple_task, x=5)
        time.sleep(0.2)

        assert task_id not in tq._tasks
        tq.close()

    def test_per_task_retain_override(self):
        tq = ThreadTaskQueue(name="RetainQueue", max_workers=2, retain_results=True)

        task_id = tq.submit(simple_task, x=5, retain_result=False)
        time.sleep(0.2)

        assert task_id not in tq._tasks
        tq.close()


class TestThreadTaskQueueInitializer:
    def test_initializer_called(self):
        initialized = []

        def init_func() -> None:
            initialized.append(True)

        tq = ThreadTaskQueue(
            name="InitQueue", max_workers=1, initializer=init_func, retain_results=True
        )
        task_id = tq.submit(simple_task, x=1)
        tq.get_result(task_id, timeout=5.0)

        assert len(initialized) == 1
        tq.close()
