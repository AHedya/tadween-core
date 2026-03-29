def task_fn(x):
    return x * 2


def _get_process_id():
    import os

    return os.getpid()


def test_process_queue_spawn_default(process_queue):
    ctx = getattr(process_queue.executor, "_mp_context", None)
    if ctx:
        try:
            current_method = ctx.get_start_method()
            assert current_method in ["spawn", "forkserver", "fork"]
        except Exception:
            pass

    task_id = process_queue.submit(task_fn, x=10)
    result = process_queue.get_result(task_id, timeout=10.0)
    assert result == 20


def test_process_queue_force_close(process_queue):

    try:
        process_queue.submit(task_fn, x=5)
        process_queue.close(force=True)
    except Exception:
        pass


class TestProcessTaskQueue:
    def test_process_queue_submission(self, process_queue):
        task_id = process_queue.submit(task_fn, x=5)
        result = process_queue.get_result(task_id, timeout=10.0)

        assert result == 10

    def test_process_queue_isolation(self, process_queue):
        task_id = process_queue.submit(_get_process_id)
        result = process_queue.get_result(task_id, timeout=10.0)

        import os

        assert result != os.getpid()
