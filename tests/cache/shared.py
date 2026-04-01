from dataclasses import dataclass


@dataclass
class SimpleSchema:
    field1: str | None = None
    field2: int | None = None
    field3: str | None = None


@dataclass
class AnotherSchema:
    name: str | None = None
    value: float | None = None
