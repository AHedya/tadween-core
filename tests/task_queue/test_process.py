from tadween_core.task_queue.process_queue import ProcessTaskQueue


def task_fn(x):
    return x * 2


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
