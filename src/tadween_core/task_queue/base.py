from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypedDict, TypeVar

T = TypeVar("T")


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Timings(TypedDict):
    duration: float
    latency: float
    waiting: float


@dataclass(slots=True)
class TaskMetadata:
    task_id: str
    start_time: float
    end_time: float
    submit_time: float

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def latency(self) -> float:
        return self.end_time - self.submit_time

    @property
    def waiting(self) -> float:
        return self.start_time - self.submit_time

    @property
    def timings(self) -> Timings:
        return {
            "duration": self.duration,
            "latency": self.latency,
            "waiting": self.waiting,
        }

    @property
    def timings_str(self) -> str:
        return f"Duration: {self.duration:.6f}, Waiting: {self.waiting:.6f}, Latency: {self.latency:.6f}"


@dataclass(slots=True, frozen=True)
class TaskEnvelope(Generic[T]):
    """The standard return type from worker processes."""

    payload: T | None
    metadata: TaskMetadata
    error: Exception | None = None
    success: bool = True
