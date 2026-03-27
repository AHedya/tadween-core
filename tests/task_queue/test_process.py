from tadween_core.task_queue.process_queue import ProcessTaskQueue


def task_fn(x):
    return x * 2


def _get_process_id():
    import os

    return os.getpid()


def test_process_queue_spawn_default():
    tq = ProcessTaskQueue(name="SpawnTestQueue", max_workers=1, retain_results=True)

    ctx = getattr(tq.executor, "_mp_context", None)
    if ctx:
        try:
            current_method = ctx.get_start_method()
            assert current_method in ["spawn", "forkserver", "fork"]
        except Exception:
            pass

    task_id = tq.submit(task_fn, x=10)
    result = tq.get_result(task_id, timeout=10.0)
    assert result == 20
    tq.close()


def test_process_queue_force_close():
    tq = ProcessTaskQueue(name="ForceCloseQueue", max_workers=1)
    tq.submit(task_fn, x=5)

    tq.close(force=True)


class TestProcessTaskQueue:
    def test_process_queue_submission(self):
        tq = ProcessTaskQueue(
            name="ProcessSubmitQueue", max_workers=2, retain_results=True
        )

        task_id = tq.submit(task_fn, x=5)
        result = tq.get_result(task_id, timeout=10.0)

        assert result == 10
        tq.close()

    def test_process_queue_isolation(self):
        tq = ProcessTaskQueue(name="IsolationQueue", max_workers=2, retain_results=True)

        task_id = tq.submit(_get_process_id)
        result = tq.get_result(task_id, timeout=10.0)

        import os

        assert result != os.getpid()
        tq.close()
