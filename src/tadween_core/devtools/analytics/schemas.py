import datetime
import json
from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from enum import IntEnum
from typing import Any, NamedTuple

import polars as pl

CHUNKS_PER_RAM_SAMPLE = 5
RAM_HEADER = "#SESSION"
RAM_SEP = "|"

CHUNKS_PER_RUNTIME_SAMPLE = 4
RUNTIME_HEADER = "#SESSION"
RUNTIME_SEP = "|"

MEMORY_HEADER = "#MEMORY_SESSION"
MEMORY_SEP = "|"


class MemoryRole(IntEnum):
    MAIN = 0
    CHILD = 1


@dataclass(slots=True, frozen=True)
class MemoryMetadata:
    title: str
    main_interval: float
    children_interval: float
    main_collector: str
    children_collector: str
    pid: int
    include_children: bool
    description: str | None = None
    date: str | None = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat()
    )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_str(cls, string: str) -> "MemoryMetadata":
        return cls.from_dict(json.loads(string))

    @classmethod
    def from_dict(cls, mapping: dict[str, Any]) -> "MemoryMetadata":
        return cls(**mapping)


@dataclass(slots=True, frozen=True)
class MemorySampleMain:
    time_lapsed: float
    usage_mb: float
    role: int = MemoryRole.MAIN

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_sequence(cls, seq: Sequence) -> "MemorySampleMain":
        return cls(
            time_lapsed=float(seq[1]),
            usage_mb=float(seq[2]),
        )


@dataclass(slots=True, frozen=True)
class MemorySampleChildren:
    time_lapsed: float
    usage_mb: float
    count: int
    role: int = MemoryRole.CHILD

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_sequence(cls, seq: Sequence) -> "MemorySampleChildren":
        return cls(
            time_lapsed=float(seq[1]),
            usage_mb=float(seq[2]),
            count=int(seq[3]),
        )


class MemorySession(NamedTuple):
    meta: MemoryMetadata
    samples: Sequence[MemorySampleMain | MemorySampleChildren]

    def __repr__(self):
        return f"<MemorySession samples={len(self.samples)} title={self.meta.title} pid={self.meta.pid}>"


@dataclass(slots=True)
class MemoryMetric:
    meta: MemoryMetadata
    data: pl.DataFrame
    id: Any | None = None

    _peak: float | None = field(default=None, init=False, repr=False)
    _avg: float | None = field(default=None, init=False, repr=False)

    @property
    def peak(self):
        if self._peak is None:
            self._peak = self.data["total_mb"].max()
        return self._peak

    @property
    def avg(self):
        if self._avg is None:
            self._avg = self.data["total_mb"].mean()
        return self._avg


@dataclass(slots=True, frozen=True)
class RAMMetadata:
    title: str
    metric_collector: str
    sampling_interval: float
    pid: int
    children_included: bool
    description: str | None = None
    date: str | None = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat()
    )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_str(cls, string: str) -> "RAMMetadata":
        return cls.from_dict(json.loads(string))

    @classmethod
    def from_dict(cls, mapping: dict[str, Any]) -> "RAMMetadata":
        return cls(**mapping)


@dataclass(slots=True, frozen=True)
class RAMSample:
    time_lapsed: float
    total_mb: float
    main_mb: float
    children_mb: float
    children_count: int

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_str(cls, sample_str: str, sep: str = RAM_SEP) -> "RAMSample":
        seq = sample_str.split(sep)
        if len(seq) != CHUNKS_PER_RAM_SAMPLE:
            raise ValueError(
                f"Can't convert to RAMSample. Expected {CHUNKS_PER_RAM_SAMPLE} chunks, got: {len(seq)}"
            )
        return cls.from_sequence(seq)

    @classmethod
    def from_sequence(cls, seq: Sequence) -> "RAMSample":
        return cls(
            time_lapsed=float(seq[0]),
            total_mb=float(seq[1]),
            main_mb=float(seq[2]),
            children_mb=float(seq[3]),
            children_count=int(seq[4]),
        )


class RAMSession(NamedTuple):
    meta: RAMMetadata
    samples: Sequence[RAMSample]

    def __repr__(self):
        return f"<RAMSession pid={self.meta.pid} samples={len(self.samples)} title={self.meta.title}>"


@dataclass(slots=True)
class RAMMetric:
    meta: RAMMetadata
    data: pl.DataFrame
    id: Any | None = None

    _peak: float | None = field(default=None, init=False, repr=False)
    _avg: float | None = field(default=None, init=False, repr=False)

    @property
    def peak(self):
        if self._peak is None:
            self._peak = self.data["total_mb"].max()
        return self._peak

    @property
    def avg(self):
        if self._avg is None:
            self._avg = self.data["total_mb"].mean()
        return self._avg


@dataclass(slots=True, frozen=True)
class RuntimeMetadata:
    title: str
    description: str | None = None
    date: str | None = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat()
    )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_str(cls, string: str) -> "RuntimeMetadata":
        return cls.from_dict(json.loads(string))

    @classmethod
    def from_dict(cls, mapping: dict[str, Any]) -> "RuntimeMetadata":
        return cls(**mapping)


@dataclass(slots=True)
class RuntimeSample:
    task_id: str
    stage: str
    waiting: float
    duration: float

    @classmethod
    def from_str(cls, sample_str: str, sep: str = RUNTIME_SEP) -> "RuntimeSample":
        seq = sample_str.split(sep)
        if len(seq) != CHUNKS_PER_RUNTIME_SAMPLE:
            raise ValueError(
                f"Can't convert to RAMSample. Expected {CHUNKS_PER_RUNTIME_SAMPLE} chunks, got: {len(seq)}"
            )
        return cls.from_sequence(seq)

    @classmethod
    def from_sequence(cls, seq: Sequence) -> "RuntimeSample":
        return cls(
            task_id=seq[0],
            stage=seq[1],
            waiting=float(seq[2]),
            duration=float(seq[3]),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_sample_record(self) -> str:
        return RUNTIME_SEP.join(
            [
                self.task_id,
                self.stage,
                str(round(self.waiting, 6)),
                str(round(self.duration, 6)),
            ]
        )

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}


class RuntimeSession(NamedTuple):
    meta: RuntimeMetadata
    samples: Sequence[RuntimeSample]

    def __repr__(self):
        return f"<TaskTiming samples={len(self.samples)} title={self.meta.title}>"


@dataclass(slots=True)
class RuntimeMetric:
    meta: RuntimeMetadata
    data: pl.DataFrame
    id: Any | None = None
