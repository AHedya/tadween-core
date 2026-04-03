import time
from queue import Queue

import pytest

from tadween_core.task_queue.base import TaskEnvelope, TaskStatus

from .shared import add_task, failing_task, fast_task, slow_task


class TaskQueueContract:
    """
    Defines the mandatory behavior for any TaskQueue implementation.
    Classes inheriting from this must provide a 'queue' fixture.
    Each implementation must pass these contract tests and can add
    implementation-specific tests in their own test module.
    """

    def test_submit_and_get_result(self, queue):
        task_id = queue.submit(fast_task, x=10)
        result = queue.get_result(task_id, timeout=5.0)
        assert result == 20

    def test_submit_with_multiple_args(self, queue):

        task_id = queue.submit(add_task, a=5, b=3)
        result = queue.get_result(task_id, timeout=5.0)

        assert result == 8

    def test_submit_multiple_tasks(self, queue):
        task_ids = [queue.submit(fast_task, x=i) for i in range(5)]
        results = [queue.get_result(tid, timeout=5.0) for tid in task_ids]
        assert results == [0, 2, 4, 6, 8]

    def test_wait_all_blocks_until_complete(self, queue):
        queue.submit(slow_task, duration=0.1)
        queue.submit(slow_task, duration=0.1)

        start = time.perf_counter()
        queue.wait_all(timeout=5.0)
        duration = time.perf_counter() - start

        assert duration >= 0.1
        assert duration < 2.0

    def test_wait_all_timeout_raises(self, queue):
        queue.submit(slow_task, duration=0.5)
        queue.submit(slow_task, duration=0.5)

        with pytest.raises(TimeoutError):
            queue.wait_all(timeout=0.2)

    def test_wait_all_empty_queue(self, queue):
        queue.wait_all(timeout=1.0)

    def test_stream_completed_yields_results(self, queue):
        n = 5
        for i in range(n):
            queue.submit(fast_task, x=i)

        results = {}
        for task_id, result in queue.stream_completed(timeout=5.0):
            results[task_id] = result

        assert len(results) == n

    def test_failing_task_propagates_error(self, queue):
        task_id = queue.submit(failing_task)

        with pytest.raises((ValueError, TimeoutError), match="Task failed!"):
            queue.get_result(task_id, timeout=1.0)

    def test_stream_completed_yields_errors(self, queue):
        queue.submit(failing_task, msg="error1")
        queue.submit(fast_task, x=1)

        errors = []
        successes = []
        for task_id, result in queue.stream_completed(timeout=5.0):
            if isinstance(result, Exception):
                errors.append((task_id, result))
            else:
                successes.append((task_id, result))

        assert len(errors) == 1
        assert len(successes) == 1

    def test_stream_completed_timeout_stops(self, queue):
        queue.submit(slow_task, duration=0.1)

        results = list(queue.stream_completed(timeout=5.0))
        assert len(results) == 1

    def test_get_status_running_then_completed(self, queue):
        task_id = queue.submit(slow_task, duration=0.3)

        status_running = queue.get_status(task_id)
        assert status_running == TaskStatus.RUNNING

        queue.wait_all(timeout=5.0)

        status_done = queue.get_status(task_id)
        assert status_done == TaskStatus.COMPLETED

    def test_get_status_failed(self, queue):
        task_id = queue.submit(failing_task, msg="fail")

        queue.wait_all(timeout=5.0)

        status = queue.get_status(task_id)
        assert status == TaskStatus.FAILED

    def test_get_status_unknown_task_raises(self, queue):
        with pytest.raises(ValueError, match="Unknown task ID"):
            queue.get_status("non-existent-id")

    def test_cancel_nonexistent_task_returns_false(self, queue):
        result = queue.cancel("nonexistent")
        assert result is False

    def test_get_all_statuses_multiple_tasks(self, queue):
        t1 = queue.submit(fast_task, x=1)
        t2 = queue.submit(slow_task, duration=0.2)
        t3 = queue.submit(failing_task, msg="err")

        queue.wait_all(timeout=2.0)

        statuses = queue.get_all_statuses()

        assert len(statuses) == 3
        assert statuses[t1] == TaskStatus.COMPLETED
        assert statuses[t2] == TaskStatus.COMPLETED
        assert statuses[t3] == TaskStatus.FAILED

    def test_task_envelope_contains_timing(self, queue):
        task_id = queue.submit(fast_task, x=10)
        time.sleep(0.1)

        with queue._lock:
            future = queue._tasks[task_id]

        envelope: TaskEnvelope = future.result(timeout=5.0)
        assert envelope.success is True
        assert envelope.payload == 20
        assert envelope.metadata.start_time > 0
        assert envelope.metadata.end_time >= envelope.metadata.start_time
        assert envelope.metadata.submit_time > 0

    def test_repr_includes_name(self, queue):
        repr_str = repr(queue)
        assert queue.name in repr_str

    def test_repr_shows_closed_state(self, queue):
        queue.close()
        assert queue._closed
        repr_str = repr(queue)
        assert "closed=True" in repr_str

    def test_submit_after_close_raises(self, queue):
        try:
            queue.close()
            with pytest.raises(RuntimeError, match="is closed"):
                queue.submit(fast_task, x=1)
        finally:
            if not queue._closed:
                queue.close()

    ## callbacks
    def test_callback_on_submit(self, queue):
        submitted = []

        def on_submit_cb(task_id: str) -> None:
            submitted.append(task_id)

        task_id = queue.submit(fast_task, x=5, on_submit=on_submit_cb)
        queue.get_result(task_id, timeout=5.0)

        assert len(submitted) == 1
        assert submitted[0] == task_id

    def test_callback_on_done(self, queue, event):

        def done_cb(task_id, future):  # noqa: ARG001
            event.set()

        task_id = queue.submit(fast_task, x=3, on_done=done_cb)
        queue.get_result(task_id, timeout=5.0)

        assert event.wait(timeout=3.0)

    def test_callback_on_done_passes_result(self, queue, event, pipe: Queue):

        def on_done_cb(task_id: str, future) -> None:  # noqa: ARG001
            envelope = future.result()
            if envelope.success:
                pipe.put(envelope.payload)
                event.set()

        task_id = queue.submit(fast_task, x=7, on_done=on_done_cb)
        queue.get_result(task_id, timeout=5.0)

        event.wait(timeout=5.0)
        assert pipe.get() == 14

    def test_callback_on_running(elf, queue, event):

        def on_running_cb(task_id: str) -> None:  # noqa: ARG001
            event.set()

        task_id = queue.submit(fast_task, x=2, on_running=on_running_cb)
        queue.get_result(task_id, timeout=5.0)

        assert event.wait(timeout=1.0)
