from collections.abc import Callable, Generator
from pathlib import Path
from typing import TypeVar

from tadween_core.devtools.analytics.schemas import (  # noqa
    MEMORY_HEADER,
    MEMORY_SEP,
    RAM_HEADER,
    RAM_SEP,
    RUNTIME_HEADER,
    RUNTIME_SEP,
    MemoryMetadata,
    MemoryRole,
    MemorySampleChildren,
    MemorySampleMain,
    MemorySession,
    RAMMetadata,
    RAMSample,
    RAMSession,
    RuntimeMetadata,
    RuntimeSample,
    RuntimeSession,
)

T_Meta = TypeVar("T_Meta")
T_Sample = TypeVar("T_Sample")
T_Session = TypeVar("T_Session")


def load_memory_sessions(
    file_path: Path | str, session_header: str = MEMORY_HEADER, sep: str = MEMORY_SEP
) -> Generator[MemorySession, None, None]:
    """Loader interface for the new MemoryCollector sessions."""

    def memory_sample_factory(line: str) -> MemorySampleMain | MemorySampleChildren:
        chunks = line.split(sep)
        role = int(chunks[0])
        if role == MemoryRole.MAIN:
            return MemorySampleMain.from_sequence(chunks)
        elif role == MemoryRole.CHILD:
            return MemorySampleChildren.from_sequence(chunks)
        raise ValueError(f"Unknown role: {role}")

    return _generic_session_loader(
        file_path=file_path,
        session_header=session_header,
        sep=sep,
        meta_factory=MemoryMetadata.from_str,
        sample_factory=memory_sample_factory,
        session_factory=lambda meta, samples: MemorySession(meta, samples),
    )


def load_ram_sessions(
    file_path: Path | str, session_header: str = RAM_HEADER, sep: str = RAM_SEP
) -> Generator[RAMSession, None, None]:
    """Loader interface for RAM monitoring sessions."""
    return _generic_session_loader(
        file_path=file_path,
        session_header=session_header,
        sep=sep,
        meta_factory=RAMMetadata.from_str,
        sample_factory=RAMSample.from_str,
        session_factory=lambda meta, samples: RAMSession(meta, samples),
    )


def load_runtime_sessions(
    file_path: Path | str,
    session_header: str = RUNTIME_HEADER,
    sep: str = RUNTIME_SEP,
) -> Generator[RuntimeSession, None, None]:
    """Loader interface for Task Runtime sessions."""
    return _generic_session_loader(
        file_path=file_path,
        session_header=session_header,
        sep=sep,
        meta_factory=RuntimeMetadata.from_str,
        sample_factory=RuntimeSample.from_str,
        session_factory=lambda meta, samples: RuntimeSession(meta, samples),
    )


def _generic_session_loader(
    file_path: Path | str,
    session_header: str,
    sep: str,
    meta_factory: Callable[[str], T_Meta],
    sample_factory: Callable[[str], T_Sample],
    session_factory: Callable[[T_Meta, list[T_Sample]], T_Session],
) -> Generator[T_Session, None, None]:
    current_meta: T_Meta | None = None
    current_samples: list[T_Sample] = []

    with open(
        file_path,
        encoding="utf-8",
    ) as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            if line.startswith(session_header):
                if current_meta is not None:
                    yield session_factory(current_meta, tuple(current_samples))
                _, meta = line.split(sep, 1)
                current_meta = meta_factory(meta)
                current_samples = []
            else:
                try:
                    sample = sample_factory(line)
                    current_samples.append(sample)
                except Exception as e:
                    raise e
        if current_meta is not None:
            yield session_factory(current_meta, tuple(current_samples))
