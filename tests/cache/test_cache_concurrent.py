import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest

from tadween_core import set_logger
from tadween_core.cache.cache import Cache

set_logger(logging.INFO)


@dataclass
class SimpleSchema:
    field1: str = ""
    field2: str = ""
    field3: str = ""


@pytest.mark.xfail(
    reason="thread-safety limitation on free-threaded builds",
    raises=AssertionError,
)
class TestConcurrency:
    def test_read_quota_corruption(self):
        """
        Demonstrates corruption of read_count due to non-atomic increments.
        """
        cache = Cache(SimpleSchema)
        key = "test_key"
        cache.set_bucket(key, SimpleSchema(field1="data"))

        num_threads = 10
        reads_per_thread = 100
        total_reads = num_threads * reads_per_thread

        barrier = threading.Barrier(num_threads)

        def read_task(i):  # noqa: ARG001
            barrier.wait(3)
            proxy = cache.get_bucket(key)

            for _ in range(reads_per_thread):
                _ = proxy.field1

        futures = []
        with ThreadPoolExecutor(max_workers=num_threads) as pool:
            for i in range(num_threads):
                futures.append(pool.submit(read_task, i))
            for f in futures:
                _ = f.result()

        entry = cache.get_field_entry(key, "field1")

        assert entry.read_count == total_reads, (
            f"LOST UPDATES DETECTED. {total_reads - entry.read_count} updates lost"
        )

    def test_bucket_size_corruption(self):
        """
        Demonstrates corruption of _bucket_sizes when multiple threads write to
        different fields in the same bucket concurrently.
        """
        cache = Cache(SimpleSchema)
        key = "test_key"
        cache.set_bucket(key, SimpleSchema())

        num_threads = 10
        barrier = threading.Barrier(num_threads)

        def write_task(i):
            proxy = cache.get_bucket(key)
            # Alternate fields to increase race probability on the shared bucket size
            field = f"field{(i % 2) + 1}"
            val = "x" * (i % 50)
            barrier.wait()
            setattr(proxy, field, val)

        futures = []
        with ThreadPoolExecutor(max_workers=num_threads) as pool:
            for i in range(num_threads):
                futures.append(pool.submit(write_task, i))
            for f in futures:
                _ = f.result()

        internal = cache.get_raw_internal(key)
        expected_size = sum(e.size for e in internal.values())
        actual_size = cache._bucket_sizes.get(key)

        assert actual_size == expected_size, (
            f"BUCKET SIZE CORRUPTION: Expected {expected_size}, got {actual_size}. "
            f"Diff: {expected_size - actual_size}"
        )

    def test_max_buckets_exceeded(self):
        pass
