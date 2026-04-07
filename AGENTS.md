# AGENTS.md

This document provides guidelines for AI coding agents working in the tadween-core repository.

## Project Overview

tadween-core is a modular, embedded micro-orchestrator for building complex, stateful processing pipelines. It uses Python 3.11+ with Pydantic for type-safe data models and supports both thread and process-based concurrency.

## Build, Lint, and Test Commands

### Setup

```bash
# Install core dependencies only
uv sync

# Install with test dependencies
uv sync --group test

# Install with dev dependencies (analytics, notebooks)
uv sync --group dev

# Install with all dependency groups
uv sync --all-groups
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run tests with verbose output
uv run pytest -v -s

# Run a single test file
uv run pytest tests/path/to/test_file.py

# Run a specific test function
uv run pytest tests/path/to/test_file.py::test_function_name

# Run tests matching a pattern
uv run pytest -k "pattern_name"

```

### Linting and Formatting

```bash
# Run ruff linter
ruff check .

# Run ruff formatter (check mode)
ruff format --check .

# Auto-fix lint issues
ruff check . --fix

# Auto-format code
ruff format .
```

### Running Tests Across Python Versions (Nox)

```bash
# Run all test sessions (Python 3.11, 3.12, 3.13, 3.14)
nox -s tests

# Run lint session
nox -s lint

# Run examples session
nox -s examples
```

## Code Style Guidelines

### Imports

```python
# Standard library imports first
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

# Third-party imports second
import numpy as np
from pydantic import BaseModel

# Local imports last
from tadween_core.broker import Message
from tadween_core.exceptions import StageError
```

### Type Annotations

- Use Python 3.11+ syntax for type hints (e.g., `X | None` instead of `Optional[X]`)
- Use `Generic` with `TypeVar` for generic classes
- TypeVar naming convention: suffix `T` (e.g., `InputT`, `OutputT`, `ArtifactT`)
- Bound TypeVars to appropriate base classes: `TypeVar("T", bound=BaseModel)`

```python
from typing import Generic, TypeVar

InputT = TypeVar("InputT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)

class BaseHandler(ABC, Generic[InputT, OutputT]):
    def run(self, inputs: InputT) -> OutputT:
        pass
```

### Dataclasses and Models

- Use `@dataclass(slots=True)` for performance-critical data structures
- Use Pydantic `BaseModel` for data validation and serialization
- Use `Field(default_factory=...)` for mutable defaults

```python
from dataclasses import dataclass

@dataclass(slots=True)
class TaskMetadata:
    task_id: str
    start_time: float
    end_time: float

from pydantic import BaseModel, Field

class MyModel(BaseModel):
    name: str
    created_at: float = Field(default_factory=time.time)
```

### Naming Conventions

- **Classes**: PascalCase (e.g., `BaseHandler`, `TaskEnvelope`, `InMemoryBroker`)
- **Functions/Methods**: snake_case (e.g., `submit_message`, `resolve_inputs`)
- **Private methods**: prefix with `_` (e.g., `_detect_input_type`, `_enforce_input_type`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `S3_TEST_BUCKET`)
- **Type aliases**: PascalCase (e.g., `BaseRepo`, `PartNameT`)

### Error Handling

- Inherit from `TadweenError` base exception
- Include context as keyword arguments
- Use `raise ... from e` to preserve exception chain

```python
from tadween_core.exceptions import StageError, PolicyError

class CustomError(StageError):
    def __init__(self, message: str, stage_name: str, **context: Any):
        super().__init__(message, stage_name=stage_name, **context)

raise PolicyError(
    message="Policy resolution failed",
    stage_name=self.name,
    policy_name=self.policy.__class__.__name__,
    method="resolve_inputs",
) from e
```

### ABC Pattern

Use Abstract Base Classes for interfaces:

```python
from abc import ABC, abstractmethod

class BaseHandler(ABC, Generic[InputT, OutputT]):
    @abstractmethod
    def run(self, inputs: InputT) -> OutputT:
        pass

    def warmup(self) -> None:
        pass
```

### Docstrings

- Use triple-quoted docstrings for classes and public methods
- Document parameters, returns, and raises

```python
def load(
    self,
    artifact_id: str,
    include: Iterable[PartNameT] | Literal["all"] | None = None,
    **options: Any,
) -> ART | None:
    """
    Load an artifact.

    Args:
        artifact_id: The unique identifier
        include: Parts to include

    Returns:
        artifact object or None if not found

    Raises:
        ValueError: if any name in ``include`` is not valid.
    """
    pass
```

## Ruff Configuration

The project uses ruff with the following rules enabled:

- `UP` - pyupgrade
- `E` - pycodestyle errors
- `F` - pyflakes
- `W` - pycodestyle warnings
- `I` - isort
- `B` - flake8-bugbear
- `C4` - flake8-comprehensions
- `ARG001` - unused arguments
- `F401` - unused imports

**Ignored rules:**
- `E501` - line too long (handled by formatter)
- `B008` - function calls in argument defaults
- `W191` - indentation contains tabs
- `B904` - raise without `from e`

## Pytest Configuration

- Test paths: `tests/`
- Test file pattern: `test_*.py`
- Test class pattern: `Test*`
- Test function pattern: `test_*`
- Python path includes `src/`
- Environment variable `TESTING=1` is set

## Testing Patterns

### Test Structure

```python
import pytest
from pydantic import BaseModel

from tadween_core.handler.base import BaseHandler


class TestInput(BaseModel):
    value: int


class TestOutput(BaseModel):
    result: str


class TestHandler(BaseHandler[TestInput, TestOutput]):
    def run(self, inputs: TestInput) -> TestOutput:
        return TestOutput(result=str(inputs.value))


def test_handler_execution():
    handler = TestHandler()
    result = handler(TestInput(value=42))
    assert result.result == "42"
```

### Using Fixtures

```python
@pytest.fixture
def handler():
    return MyHandler()

def test_something(handler):
    result = handler.run(MyInput(value=1))
    assert result.success
```

### Contract Tests

Use base classes for shared test contracts:

```python
class RepoContract:
    def test_basic(self, repo):
        repo.save(artifact)
        assert repo.exists(artifact.id)
```

## Project Structure

```
src/tadween_core/
├── __init__.py
├── exceptions.py         # Custom exception hierarchy
├── broker/               # Message bus implementations
├── cache/                # Type-safe caching
├── handler/              # Computational handlers
├── repo/                 # Persistence layer
├── stage/                # Stage orchestration
├── task_queue/           # Thread/process queues
├── types/                # Core type definitions
├── utils.py              # Utility functions
└── workflow/             # DAG orchestration

tests/
├── conftest.py           # Shared fixtures
├── shared_types.py       # Test type definitions
├── repo/                 # Repository tests
├── task_queue/           # Queue tests
└── test_*.py             # Integration tests
```

## Comments Policy

Do not add comments to code unless explicitly requested. Keep code self-documenting through clear naming and structure.

## Python Version Support

- Minimum: Python 3.11
- Maximum: Python 3.14 (exclusive)
- Use modern Python features: `match`, `|` for unions, `TypeAlias`, etc.