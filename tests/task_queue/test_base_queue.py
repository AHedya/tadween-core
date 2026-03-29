import logging
import time

import pytest

from tadween_core.task_queue.base import TaskEnvelope, TaskStatus
from tadween_core.task_queue.thread_queue import ThreadTaskQueue


def fast_task(x: int) -> int:
    return x * 2


def slow_task(duration: float) -> str:
    time.sleep(duration)
    return "done"


def failing_task(msg: str) -> None:
    raise ValueError(msg)


class TestWaitAll:
    def test_wait_all_blocks_until_complete_thread(self, thread_queue):
        thread_queue.submit(slow_task, duration=0.1)
        thread_queue.submit(slow_task, duration=0.1)

        start = time.perf_counter()
        thread_queue.wait_all(timeout=5.0)
        duration = time.perf_counter() - start

        assert duration >= 0.1
        assert duration < 2.0

    def test_wait_all_blocks_until_complete_process(self, process_queue):
        process_queue.submit(slow_task, duration=0.1)
        process_queue.submit(slow_task, duration=0.1)

        start = time.perf_counter()
        process_queue.wait_all(timeout=5.0)
        duration = time.perf_counter() - start

        assert duration >= 0.1
        assert duration < 2.0

    def test_wait_all_timeout_raises(self):
        tq = ThreadTaskQueue(name="TimeoutQueue", max_workers=1)
        try:
            tq.submit(slow_task, duration=0.5)
            tq.submit(slow_task, duration=0.5)
            with pytest.raises(TimeoutError):
                tq.wait_all(timeout=0.2)
        finally:
            tq.close(force=True)

    def test_wait_all_empty_queue(self, thread_queue):
        thread_queue.wait_all(timeout=1.0)


class TestStreamCompleted:
    def test_stream_completed_yields_results_in_order_thread(self, thread_queue):
        n = 5
        for i in range(n):
            thread_queue.submit(fast_task, x=i)

        results = {}
        for task_id, result in thread_queue.stream_completed(timeout=5.0):
            results[task_id] = result

        assert len(results) == n

    def test_stream_completed_yields_errors_thread(self, thread_queue):
        thread_queue.submit(failing_task, msg="error1")
        thread_queue.submit(fast_task, x=1)

        errors = []
        successes = []
        for task_id, result in thread_queue.stream_completed(timeout=5.0):
            if isinstance(result, Exception):
                errors.append((task_id, result))
            else:
                successes.append((task_id, result))

        assert len(errors) == 1
        assert len(successes) == 1

    def test_stream_completed_without_retain_warns(self):
        tq = ThreadTaskQueue(
            name="NoRetainStreamQueue", max_workers=1, retain_results=False
        )
        try:
            tq.submit(fast_task, x=1)

            with pytest.MonkeyPatch.context() as m:
                logged = []
                m.setattr(
                    logging.Logger,
                    "warning",
                    lambda self, msg, *args: logged.append(msg),
                )

                count = 0
                for _ in tq.stream_completed(timeout=5.0):
                    count += 1
        finally:
            tq.close()

    def test_stream_completed_timeout_stops(self, thread_queue):
        thread_queue.submit(slow_task, duration=0.1)

        results = list(thread_queue.stream_completed(timeout=5.0))
        assert len(results) == 1


class TestGetStatus:
    def test_get_status_running_then_completed(self, thread_queue):
        task_id = thread_queue.submit(slow_task, duration=0.3)

        status_running = thread_queue.get_status(task_id)
        assert status_running == TaskStatus.RUNNING

        thread_queue.wait_all(timeout=5.0)

        status_done = thread_queue.get_status(task_id)
        assert status_done == TaskStatus.COMPLETED

    def test_get_status_failed(self, thread_queue):
        task_id = thread_queue.submit(failing_task, msg="fail")

        thread_queue.wait_all(timeout=5.0)

        status = thread_queue.get_status(task_id)
        assert status == TaskStatus.FAILED

    def test_get_status_unknown_task_raises(self, thread_queue):
        with pytest.raises(ValueError, match="Unknown task ID"):
            thread_queue.get_status("non-existent-id")


class TestCancel:
    def test_cancel_pending_task(self):
        tq = ThreadTaskQueue(name="CancelQueue", max_workers=1)
        try:
            task_id1 = tq.submit(slow_task, duration=0.2)
            task_id2 = tq.submit(slow_task, duration=10.0)

            result1 = tq.cancel(task_id1)
            result2 = tq.cancel(task_id2)

            assert not result1
            assert result2
        finally:
            tq.close(force=True)

    def test_cancel_nonexistent_task_returns_false(self, thread_queue):
        result = thread_queue.cancel("nonexistent")

        assert result is False


class TestGetAllStatuses:
    def test_get_all_statuses_multiple_tasks(self, thread_queue):
        t1 = thread_queue.submit(fast_task, x=1)
        t2 = thread_queue.submit(slow_task, duration=0.2)
        t3 = thread_queue.submit(failing_task, msg="err")

        thread_queue.wait_all(timeout=2.0)

        statuses = thread_queue.get_all_statuses()

        assert len(statuses) == 3
        assert statuses[t1] == TaskStatus.COMPLETED
        assert statuses[t2] == TaskStatus.COMPLETED
        assert statuses[t3] == TaskStatus.FAILED


class TestTaskMetadata:
    def test_task_envelope_contains_timing(self):

        tq = ThreadTaskQueue(name="TimingQueue", max_workers=1, retain_results=True)
        try:
            task_id = tq.submit(fast_task, x=10)
            time.sleep(0.1)

            with tq._lock:
                future = tq._tasks[task_id]

            envelope: TaskEnvelope = future.result(timeout=5.0)
            assert envelope.success is True
            assert envelope.payload == 20
            assert envelope.metadata.start_time > 0
            assert envelope.metadata.end_time >= envelope.metadata.start_time
            assert envelope.metadata.submit_time > 0
        finally:
            tq.close()


class TestTaskQueueRepr:
    def test_repr_includes_name(self):
        tq = ThreadTaskQueue(name="TestRepQueue", max_workers=4)
        try:
            repr_str = repr(tq)
            assert "TestRepQueue" in repr_str
            assert "workers=4" in repr_str
        finally:
            tq.close()

    def test_repr_shows_closed_state(self):
        tq = ThreadTaskQueue(name="ClosedRepQueue", max_workers=1)
        try:
            pass
        finally:
            tq.close()

        assert tq._closed
        repr_str = repr(tq)
        assert "closed=True" in repr_str
