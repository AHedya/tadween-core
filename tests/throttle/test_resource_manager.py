import threading
import time

import pytest

from tadween_core import ResourceManager
from tadween_core.exceptions import ResourceError


class TestResourceManagerInit:
    def test_basic_creation(self):
        rm = ResourceManager(resources={"cuda": 1, "RAM_MB": 2048})
        assert rm.capacity == {"cuda": 1, "RAM_MB": 2048}
        assert rm.available == {"cuda": 1, "RAM_MB": 2048}

    def test_empty_resources_raises(self):
        with pytest.raises(ValueError, match="at least one resource"):
            ResourceManager(resources={})

    def test_zero_capacity_raises(self):
        with pytest.raises(ValueError, match="must be positive"):
            ResourceManager(resources={"cuda": 0})

    def test_negative_capacity_raises(self):
        with pytest.raises(ValueError, match="must be positive"):
            ResourceManager(resources={"cuda": -1})


class TestAcquireRelease:
    def test_acquire_and_release(self):
        rm = ResourceManager(resources={"cuda": 2})
        rm.acquire({"cuda": 1})
        assert rm.available == {"cuda": 1}
        rm.release({"cuda": 1})
        assert rm.available == {"cuda": 2}

    def test_acquire_full_capacity(self):
        rm = ResourceManager(resources={"cuda": 1})
        rm.acquire({"cuda": 1})
        assert rm.available == {"cuda": 0}
        rm.release({"cuda": 1})
        assert rm.available == {"cuda": 1}

    def test_multi_resource_acquire(self):
        rm = ResourceManager(resources={"cuda": 2, "RAM_MB": 2048})
        rm.acquire({"cuda": 1, "RAM_MB": 500})
        assert rm.available == {"cuda": 1, "RAM_MB": 1548}
        rm.release({"cuda": 1, "RAM_MB": 500})
        assert rm.available == {"cuda": 2, "RAM_MB": 2048}

    def test_acquire_unknown_resource_raises(self):
        rm = ResourceManager(resources={"cuda": 1})
        with pytest.raises(ValueError, match="Unknown resource"):
            rm.acquire({"gpu": 1})

    def test_acquire_zero_demand_raises(self):
        rm = ResourceManager(resources={"cuda": 1})
        with pytest.raises(ValueError, match="must be positive"):
            rm.acquire({"cuda": 0})

    def test_acquire_negative_demand_raises(self):
        rm = ResourceManager(resources={"cuda": 1})
        with pytest.raises(ValueError, match="must be positive"):
            rm.acquire({"cuda": -1})

    def test_release_exceeds_capacity_raises(self):
        rm = ResourceManager(resources={"cuda": 1})
        with pytest.raises(ValueError, match="exceed capacity"):
            rm.release({"cuda": 1})

    def test_release_unknown_resource_raises(self):
        rm = ResourceManager(resources={"cuda": 1})
        with pytest.raises(ValueError, match="Unknown resource"):
            rm.release({"gpu": 1})

    def test_acquire_empty_demands_noop(self):
        rm = ResourceManager(resources={"cuda": 1})
        rm.acquire({})
        assert rm.available == {"cuda": 1}

    def test_release_empty_demands_noop(self):
        rm = ResourceManager(resources={"cuda": 1})
        rm.release({})
        assert rm.available == {"cuda": 1}

    def test_fractional_units(self):
        rm = ResourceManager(resources={"RAM_MB": 2048})
        rm.acquire({"RAM_MB": 500.5})
        assert rm.available == {"RAM_MB": 1547.5}
        rm.release({"RAM_MB": 500.5})
        assert rm.available == {"RAM_MB": 2048}


class TestBlockingAcquire:
    def test_acquire_blocks_until_available(self):
        rm = ResourceManager(resources={"cuda": 1})
        rm.acquire({"cuda": 1})

        waiting_event = threading.Event()
        acquired = threading.Event()
        done = threading.Event()

        def blocked_acquire():
            waiting_event.set()
            rm.acquire({"cuda": 1})
            acquired.set()
            done.wait(timeout=5)

        t = threading.Thread(target=blocked_acquire, daemon=True)
        t.start()

        assert waiting_event.wait(timeout=2)
        # Give a tiny bit of time to enter RM.acquire()'s while loop
        time.sleep(0.05)
        assert not acquired.is_set()

        rm.release({"cuda": 1})
        assert acquired.wait(timeout=2)
        done.set()
        t.join(timeout=2)

    def test_atomic_multi_resource_acquire(self):
        rm = ResourceManager(resources={"cuda": 1, "RAM_MB": 1000})
        rm.acquire({"cuda": 1})

        waiting_event = threading.Event()
        acquired = threading.Event()
        release_event = threading.Event()

        def blocked_acquire():
            waiting_event.set()
            rm.acquire({"cuda": 1, "RAM_MB": 500})
            acquired.set()
            release_event.wait(timeout=5)

        t = threading.Thread(target=blocked_acquire, daemon=True)
        t.start()

        assert waiting_event.wait(timeout=2)
        time.sleep(0.05)
        assert not acquired.is_set()

        rm.release({"cuda": 1})
        assert acquired.wait(timeout=2)
        assert rm.available == {"cuda": 0, "RAM_MB": 500}
        release_event.set()
        t.join(timeout=2)

    def test_multiple_waiters_woken(self):
        rm = ResourceManager(resources={"cuda": 2})
        rm.acquire({"cuda": 2})

        waiting_events = [threading.Event() for _ in range(2)]
        events = [threading.Event() for _ in range(2)]
        release_events = [threading.Event() for _ in range(2)]

        def waiter(idx):
            waiting_events[idx].set()
            rm.acquire({"cuda": 1})
            events[idx].set()
            release_events[idx].wait(timeout=5)

        threads = [
            threading.Thread(target=waiter, args=(i,), daemon=True) for i in range(2)
        ]
        for t in threads:
            t.start()

        for we in waiting_events:
            assert we.wait(timeout=2)
        time.sleep(0.05)
        assert not any(e.is_set() for e in events)

        rm.release({"cuda": 2})

        for e in events:
            assert e.wait(timeout=2)

        for re in release_events:
            re.set()
        for t in threads:
            t.join(timeout=2)


class TestShutdown:
    def test_shutdown_wakes_blocked_acquire(self):
        rm = ResourceManager(resources={"cuda": 1})
        rm.acquire({"cuda": 1})

        waiting_event = threading.Event()
        errors = []

        def blocked_acquire():
            waiting_event.set()
            try:
                rm.acquire({"cuda": 1})
            except ResourceError as e:
                errors.append(e)

        t = threading.Thread(target=blocked_acquire, daemon=True)
        t.start()

        assert waiting_event.wait(timeout=2)
        time.sleep(0.05)
        rm.shutdown()
        t.join(timeout=2)

        assert len(errors) == 1
        assert "shut down" in str(errors[0])

    def test_acquire_after_shutdown_raises(self):
        rm = ResourceManager(resources={"cuda": 1})
        rm.shutdown()
        with pytest.raises(ResourceError, match="shut down"):
            rm.acquire({"cuda": 1})

    def test_shutdown_idempotent(self):
        rm = ResourceManager(resources={"cuda": 1})
        rm.shutdown()
        rm.shutdown()
        assert rm.is_shutdown


class TestProperties:
    def test_available_snapshot(self):
        rm = ResourceManager(resources={"cuda": 2})
        snap = rm.available
        rm.acquire({"cuda": 1})
        assert snap == {"cuda": 2}
        assert rm.available == {"cuda": 1}

    def test_capacity_snapshot(self):
        rm = ResourceManager(resources={"cuda": 2})
        snap = rm.capacity
        assert snap == {"cuda": 2}
