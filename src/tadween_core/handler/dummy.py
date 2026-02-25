import hashlib
import os
import socket
import tempfile
import time
import zlib
from typing import Any

import numpy as np
from pydantic import BaseModel, Field

from tadween_core.handler.base import BaseHandler


class MockDownloadInput(BaseModel):
    """
    Input schema for mock download operations.

    This simulates downloading by generating realistic I/O and CPU load
    without actually making network requests.
    """

    file_size_mb: float = Field(..., description="Simulated file size in MB")
    bandwidth_mbps: float = Field(
        10.0, description="Simulated network bandwidth in Mbps"
    )
    latency_ms: float = Field(
        50.0, ge=0, description="Simulated network latency in milliseconds"
    )
    chunk_size: int = Field(
        64 * 1024, description="Chunk size in bytes (64KB is realistic for HTTP)"
    )
    cpu_overhead: float = Field(
        0.05,
        ge=0.0,
        le=1.0,
        description="Fraction of time spent on CPU work (0.05 = 5%)",
    )
    enable_compression: bool = Field(
        True, description="Simulate compression/decompression (CPU work)"
    )
    enable_checksum: bool = Field(
        True, description="Compute SHA256 checksum (CPU work)"
    )


class MockDownloadOutput(BaseModel):
    """Output schema for mock download operations."""

    data: bytes = Field(..., description="Generated data")
    size_bytes: int = Field(..., description="Actual size in bytes")
    duration_seconds: float = Field(..., description="Simulated download duration")
    checksum_sha256: str | None = Field(None, description="SHA256 checksum of data")
    simulated: bool = Field(
        True, description="Flag indicating this was a mock download"
    )

    def __repr__(self) -> str:
        """Custom repr that suppresses bytes output."""
        return (
            f"<MockDownloadOutput\n"
            f"  size_bytes={self.size_bytes:,},\n"
            f"  duration_seconds={self.duration_seconds:.3f},\n"
            f"  checksum_sha256='{self.checksum_sha256[:16] if self.checksum_sha256 else None}...',\n"
            f"  simulated={self.simulated},\n"
            f"  data=<{len(self.data)} bytes suppressed>\n"
            f">"
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        speed_mbps = (self.size_bytes / self.duration_seconds) / (1024 * 1024)
        return (
            f"<MockDownloadOutput:\n"
            f"  Size: {self.size_bytes:,} bytes ({self.size_bytes / (1024 * 1024):.2f} MB)\n"
            f"  Duration: {self.duration_seconds:.3f}s\n"
            f"  Speed: {speed_mbps:.2f} MB/s\n"
            f"  Checksum: {self.checksum_sha256[:16] if self.checksum_sha256 else 'N/A'}...\n"
            f"  Simulated: {self.simulated}>"
        )


class MockDownloadHandler(BaseHandler[MockDownloadInput, MockDownloadOutput]):
    """
    Mock download handler for testing and development.

    ⚠️  THIS IS A TEST DOUBLE - NOT A REAL DOWNLOAD HANDLER ⚠️

    This handler simulates realistic HTTP download behavior WITHOUT making
    actual network requests. It's designed for:

    1. Testing workflow orchestration
    2. Testing threading/GIL behavior
    3. Development without external dependencies
    4. Benchmarking and performance testing

    What it simulates accurately:
    - I/O operations (socket, disk) that release the GIL
    - CPU work (compression, checksums) that holds the GIL
    - Bandwidth limitations and network latency
    - Chunked data transfer patterns
    - Realistic timing and resource usage

    What it does NOT do:
    - Make actual HTTP requests
    - Download from real URLs
    - Handle network errors/retries
    - Validate SSL certificates

    For production use, use DownloadHandler instead.

    Example:
        >>> handler = MockDownloadHandler()
        >>> result = handler.run(MockDownloadInput(
        ...     file_size_mb=5.0,
        ...     bandwidth_mbps=20.0
        ... ))
        >>> print(f"Simulated download: {result.size_bytes} bytes "
        ...       f"in {result.duration_seconds:.2f}s")
    """

    def __init__(self):
        # No resources to initialize - this is stateless
        pass

    def run(self, inputs: MockDownloadInput) -> MockDownloadOutput:
        """
        Simulate download with realistic I/O and CPU operations.

        Args:
            inputs: Mock download configuration

        Returns:
            Mock download result with generated data and timing
        """
        start_time = time.perf_counter()

        total_bytes = int(inputs.file_size_mb * 1024 * 1024)
        downloaded = 0

        # Calculate timing parameters
        bytes_per_second = (inputs.bandwidth_mbps * 1024 * 1024) / 8
        chunk_delay = inputs.chunk_size / bytes_per_second

        # 1. SIMULATE INITIAL CONNECTION LATENCY (I/O wait, releases GIL)
        if inputs.latency_ms > 0:
            self._simulate_network_latency(inputs.latency_ms)

        # Use temporary file for realistic disk I/O operations
        with tempfile.NamedTemporaryFile(delete=True, buffering=0) as tmpfile:
            # Initialize checksum if enabled
            hasher = hashlib.sha256() if inputs.enable_checksum else None

            # Download in chunks
            while downloaded < total_bytes:
                # Calculate current chunk size (last chunk might be smaller)
                current_chunk_size = min(inputs.chunk_size, total_bytes - downloaded)

                # 2. NETWORK I/O: Read data from socket (releases GIL)
                chunk_data = self._simulate_network_read(
                    current_chunk_size, chunk_delay
                )

                # 3. CPU WORK: Decompress data (holds GIL)
                if inputs.enable_compression:
                    chunk_data = self._simulate_decompression(chunk_data)

                # 4. CPU WORK: Update checksum (holds GIL)
                if hasher:
                    hasher.update(chunk_data)

                # 5. DISK I/O: Write to file (releases GIL)
                self._simulate_disk_write(tmpfile, chunk_data)

                # 6. CPU WORK: Memory operations overhead (holds GIL)
                self._simulate_cpu_overhead(
                    len(chunk_data), bytes_per_second, inputs.cpu_overhead
                )

                downloaded += len(chunk_data)

            # Read back complete file (simulates returning response body)
            tmpfile.seek(0)
            result_data = tmpfile.read()

        duration = time.perf_counter() - start_time

        return MockDownloadOutput(
            data=result_data,
            size_bytes=len(result_data),
            duration_seconds=duration,
            checksum_sha256=hasher.hexdigest() if hasher else None,
            simulated=True,
        )

    def warmup(self) -> None:
        pass

    def _simulate_network_latency(self, latency_ms: float) -> None:
        """Simulate network latency using actual socket operation."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(latency_ms / 1000.0)
        try:
            sock.recvfrom(1)  # Blocking I/O (releases GIL)
        except TimeoutError:
            pass
        finally:
            sock.close()

    def _simulate_network_read(self, size: int, delay: float) -> bytes:
        """Simulate reading data from network socket."""
        # Simulate bandwidth limitation with socket I/O
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(delay)

        try:
            sock.recvfrom(1)  # Blocking I/O (releases GIL)
        except TimeoutError:
            pass
        finally:
            sock.close()

        # Generate random data (os.urandom is I/O bound)
        return os.urandom(size)

    def _simulate_decompression(self, data: bytes) -> bytes:
        """Simulate CPU-intensive decompression work (holds GIL)."""
        compressed = zlib.compress(data, level=1)
        decompressed = zlib.decompress(compressed)
        return decompressed

    def _simulate_disk_write(self, file_obj, data: bytes) -> None:
        """Simulate writing to disk (releases GIL)."""
        file_obj.write(data)
        os.fsync(file_obj.fileno())  # Force actual disk I/O

    def _simulate_cpu_overhead(
        self, data_size: int, bytes_per_second: float, cpu_overhead: float
    ) -> None:
        """Simulate CPU overhead for memory operations (holds GIL)."""
        cpu_time = (data_size / bytes_per_second) * cpu_overhead

        if cpu_time > 0.001:  # Only if significant
            end_time = time.perf_counter() + cpu_time
            dummy = 0
            # Actual CPU work (holds GIL)
            while time.perf_counter() < end_time:
                dummy = hashlib.md5(str(dummy).encode()).digest()

    def shutdown(self) -> None:
        pass


class CPUBoundInput(BaseModel):
    """Input for CPU-intensive computation."""

    duration_seconds: float = Field(..., description="Target computation duration")
    complexity: str = Field(
        "medium", description="Computation complexity: 'light', 'medium', 'heavy'"
    )
    iterations: int | None = Field(
        None, description="Override auto-calculated iterations"
    )


class CPUBoundOutput(BaseModel):
    """Output from CPU-bound computation."""

    result_hash: str = Field(..., description="Hash of computation result")
    iterations: int = Field(..., description="Number of iterations performed")
    duration_seconds: float = Field(..., description="Actual execution duration")
    workload_type: str = Field("cpu_bound", description="Workload classification")

    def __repr__(self) -> str:
        return (
            f"CPUBoundOutput(\n"
            f"  iterations={self.iterations:,},\n"
            f"  duration_seconds={self.duration_seconds:.3f},\n"
            f"  result_hash='{self.result_hash[:16]}...',\n"
            f"  workload_type='{self.workload_type}'\n"
            f")"
        )

    def __str__(self) -> str:
        ops_per_sec = self.iterations / self.duration_seconds
        return (
            f"CPUBoundOutput:\n"
            f"  Type: {self.workload_type}\n"
            f"  Iterations: {self.iterations:,}\n"
            f"  Duration: {self.duration_seconds:.3f}s\n"
            f"  Rate: {ops_per_sec:,.0f} ops/sec\n"
            f"  Result: {self.result_hash[:16]}..."
        )


class CPUBoundHandler(BaseHandler[CPUBoundInput, CPUBoundOutput]):
    """
    CPU-bound handler performing intensive computation.

    ⚠️ TEST HANDLER - Pure CPU work for benchmarking.

    GIL behavior: Holds GIL throughout execution (Python computation)
    Threading: Does NOT parallelize (GIL contention)
    Use case: Testing CPU-heavy workflows, ML inference, data processing

    This handler performs cryptographic hashing in a loop to simulate
    CPU-intensive work. The work is purely computational and holds the
    GIL, making it unsuitable for threading but ideal for multiprocessing.
    """

    # Complexity multipliers for iteration count
    COMPLEXITY_MULTIPLIERS = {
        "light": 0.5,
        "medium": 1.0,
        "heavy": 2.0,
    }

    def run(self, inputs: CPUBoundInput) -> CPUBoundOutput:
        start_time = time.perf_counter()

        # Calculate iterations based on complexity and duration
        if inputs.iterations is not None:
            iterations = inputs.iterations
        else:
            # Approximate iterations needed for target duration
            # Calibrated: ~500k iterations ≈ 1 second on typical hardware
            multiplier = self.COMPLEXITY_MULTIPLIERS.get(inputs.complexity, 1.0)
            iterations = int(500_000 * inputs.duration_seconds * multiplier)

        # Perform CPU-intensive work (holds GIL)
        result = self._compute_hash_chain(iterations)

        duration = time.perf_counter() - start_time

        return CPUBoundOutput(
            result_hash=result,
            iterations=iterations,
            duration_seconds=duration,
            workload_type="cpu_bound",
        )

    def _compute_hash_chain(self, iterations: int) -> str:
        """
        Compute a chain of cryptographic hashes.
        This is pure CPU work that holds the GIL.
        """
        result = b"0"
        for i in range(iterations):
            # Alternate between SHA256 (slower) and MD5 (faster)
            if i % 3 == 0:
                result = hashlib.sha256(result).digest()
            else:
                result = hashlib.md5(result).digest()

        return hashlib.sha256(result).hexdigest()


class SleepInput(BaseModel):
    """Input for sleep/wait operations."""

    duration_seconds: float = Field(..., description="Sleep duration in seconds")
    use_busy_wait: bool = Field(
        False, description="Use busy-wait loop instead of sleep (holds GIL)"
    )


class SleepOutput(BaseModel):
    """Output from sleep operations."""

    requested_duration: float = Field(..., description="Requested sleep duration")
    actual_duration: float = Field(..., description="Actual elapsed time")
    workload_type: str = Field("sleep", description="Workload classification")

    def __repr__(self) -> str:
        return (
            f"SleepOutput(\n"
            f"  requested_duration={self.requested_duration:.3f},\n"
            f"  actual_duration={self.actual_duration:.3f},\n"
            f"  workload_type='{self.workload_type}'\n"
            f")"
        )

    def __str__(self) -> str:
        accuracy = (self.actual_duration / self.requested_duration) * 100
        return (
            f"SleepOutput:\n"
            f"  Type: {self.workload_type}\n"
            f"  Requested: {self.requested_duration:.3f}s\n"
            f"  Actual: {self.actual_duration:.3f}s\n"
            f"  Accuracy: {accuracy:.1f}%"
        )


class SleepHandler(BaseHandler[SleepInput, SleepOutput]):
    """
    Sleep handler for pure waiting with minimal overhead.

    ⚠️ TEST HANDLER - Simple sleep for timing tests.

    GIL behavior: Releases GIL during sleep (by default)
    Threading: Parallelizes perfectly (sleep releases GIL)
    Use case: Testing timing, orchestration delays, rate limiting

    Note: This is fundamentally different from I/O or CPU handlers.
    It does NO actual work - just waits. Use this to test workflow
    timing, delays, and to verify that simple sleep() is not sufficient
    for testing realistic I/O patterns.
    """

    def run(self, inputs: SleepInput) -> SleepOutput:
        start_time = time.perf_counter()

        if inputs.use_busy_wait:
            # Busy wait - holds GIL (for comparison)
            end_time = start_time + inputs.duration_seconds
            while time.perf_counter() < end_time:
                pass  # Spin loop holds GIL
        else:
            # Normal sleep - releases GIL
            time.sleep(inputs.duration_seconds)

        actual_duration = time.perf_counter() - start_time

        return SleepOutput(
            requested_duration=inputs.duration_seconds,
            actual_duration=actual_duration,
            workload_type="sleep_busy" if inputs.use_busy_wait else "sleep",
        )


class SumSquaresInput(BaseModel):
    n_elements: int = Field(..., gt=0, description="Number of elements to process")


class SumSquaresOutput(BaseModel):
    result: float = Field(..., description="The calculated sum of squares")
    duration: float = Field(..., description="Time taken in seconds")
    handler_type: str = Field(..., description="Type of implementation used")

    def __str__(self) -> str:
        return f"<SumSquaresOutput: Type: {self.handler_type} Duration: {self.duration:.3f} result: {self.result:,}>"


class PythonSumHandler(BaseHandler[SumSquaresInput, SumSquaresOutput]):
    """
    CPU-bound: Processes data using Python's native loops.
    The GIL is held throughout the entire execution.
    """

    def run(self, inputs: SumSquaresInput) -> SumSquaresOutput:
        # Create a list of Python float objects
        data = [float(i) for i in range(inputs.n_elements)]

        start_time = time.perf_counter()

        # Computation: Pure Python loop holding the GIL
        total = 0.0
        for x in data:
            total += x * x

        duration = time.perf_counter() - start_time

        return SumSquaresOutput(
            result=total,
            duration=duration,
            handler_type="pure_python",
        )


class NumpySumHandler(BaseHandler[SumSquaresInput, SumSquaresOutput]):
    """
    CPU-bound: Processes data using NumPy's C-extension.
    The GIL is released during the heavy computation phase.
    """

    def run(self, inputs: SumSquaresInput) -> SumSquaresOutput:
        # Create a contiguous C-array of doubles
        data = np.arange(inputs.n_elements, dtype=np.float64)
        start_time = time.perf_counter()

        total = np.sum(np.square(data))

        duration = time.perf_counter() - start_time
        return SumSquaresOutput(
            result=float(total), duration=duration, handler_type="numpy"
        )


class PrintInput(BaseModel):
    inputs: Any | None = None


class PrintOutput(BaseModel):
    outputs: Any | None = None


class PrintHandler(BaseHandler[PrintInput, PrintOutput]):
    def run(self, inputs):
        print("Printing...")
        if inputs.inputs is not None:
            print(inputs.inputs)
        else:
            pass
        return PrintOutput()
