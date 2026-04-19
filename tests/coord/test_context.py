import threading
import time

import pytest

from tadween_core.coord import WorkflowContext


class TestWorkflowContext:
    def test_basic_notify(self):
        ctx = WorkflowContext()
        results = []

        def waiter():
            ctx.wait_for("test_event", lambda ctx: "go" not in ctx.state)
            with ctx._lock:
                results.append(ctx.state["go"])

        t = threading.Thread(target=waiter, daemon=True)
        t.start()

        time.sleep(0.05)
        assert len(results) == 0

        with ctx._lock:
            ctx.state["go"] = "success"
        ctx.notify("test_event")

        t.join(timeout=2)
        assert results == ["success"]

    def test_state_increment_decrement(self):
        ctx = WorkflowContext()

        assert ctx.state_get("counter", 0) == 0
        assert ctx.increment("counter") == 1
        assert ctx.state_get("counter", 0) == 1
        assert ctx.increment("counter", 5) == 6
        assert ctx.decrement("counter", 2) == 4
        assert ctx.state_get("counter", 0) == 4

        ctx.apply_state({"counter": -4, "other": 10})
        assert ctx.state_get("counter", 0) == 0
        assert ctx.state_get("other", 0) == 10

    def test_apply_state_atomicity(self):
        ctx = WorkflowContext()

        def incrementer(key, n):
            for _ in range(n):
                ctx.increment(key)

        threads = [
            threading.Thread(target=incrementer, args=("x", 1000)) for _ in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert ctx.state_get("x", 0) == 10000

    def test_directed_notification(self):
        ctx = WorkflowContext()
        results = []

        def waiter(event, key):
            ctx.wait_for(event, lambda ctx: key not in ctx.state)
            with ctx._lock:
                results.append(key)

        t1 = threading.Thread(target=waiter, args=("event1", "key1"), daemon=True)
        t2 = threading.Thread(target=waiter, args=("event2", "key2"), daemon=True)
        t1.start()
        t2.start()

        time.sleep(0.05)

        with ctx._lock:
            ctx.state["key1"] = True
            ctx.state["key2"] = True

        # Notify only event1
        ctx.notify("event1")
        t1.join(timeout=2)
        assert "key1" in results
        assert "key2" not in results

        # Notify event2
        ctx.notify("event2")
        t2.join(timeout=2)
        assert "key2" in results

    def test_poll_heartbeat(self):
        # Test that it wakes up even without notification if poll_interval elapses
        # wait_for blocks while predicate is True, returns when False
        ctx = WorkflowContext()
        # "ready" not in state initially, so "ready" not in ctx.state is True
        # wait_for will block while predicate is True, then return when
        # "ready" is added (predicate becomes False)
        start = time.monotonic()

        def set_ready_later():
            time.sleep(0.3)
            with ctx._lock:
                ctx.state["ready"] = True

        threading.Thread(target=set_ready_later, daemon=True).start()

        # Predicate is True (key missing) -> blocks. Once key added, becomes False -> returns
        ctx.wait_for("test", lambda ctx: "ready" not in ctx.state, poll_interval=0.2)
        elapsed = time.monotonic() - start

        assert ctx.state["ready"]
        # Allow some room for OS scheduling.
        assert 0.3 <= elapsed < 10.0

    def test_timeout_raises(self):
        ctx = WorkflowContext()
        with pytest.raises(TimeoutError, match="timeout"):
            ctx.wait_for("test", lambda _: True, timeout=0.1, poll_interval=0.05)

    def test_shutdown_wakes_all(self):
        ctx = WorkflowContext()
        errors = []

        def waiter():
            try:
                ctx.wait_for("test", lambda _: True)
            except RuntimeError as e:
                errors.append(e)

        t = threading.Thread(target=waiter, daemon=True)
        t.start()

        time.sleep(0.05)
        ctx.shutdown()
        t.join(timeout=1)

        assert len(errors) == 1
        assert "shut down" in str(errors[0])
