from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

InputT = TypeVar("InputT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)


class BaseHandler(ABC, Generic[InputT, OutputT]):
    """
    Base class for all computational handlers.

    Handlers are stateful objects that:
    1. Accept typed input (Pydantic model).
    2. Perform computation (CPU, GPU, API call, I/O).
    3. Return typed output (Pydantic model).
    4. Maintain state (loaded models, connections) across executions.

    Lifecycle:
    - __init__: Lightweight configuration (store config, do NOT load heavy resources).
    - warmup: Explicitly load heavy resources (optional, but recommended).
    - run: Execute the logic (lazy-loads resources if not warmed up).
    - shutdown: Cleanup resources (close connections, free VRAM).
    """

    @abstractmethod
    def run(self, inputs: InputT) -> OutputT:
        """
        Execute handler logic.
        Args:
            inputs: Validated input model.
        Returns:
            Validated output model.
        """
        pass

    def warmup(self) -> None:
        """
        Optional: Pre-load heavy resources (models, DB connections).
        Override this to prevent latency on the first 'run' call.
        """
        pass

    def shutdown(self) -> None:
        """
        Optional: Release resources.
        """
        pass

    def __call__(self, inputs: InputT) -> OutputT:
        """
        Syntactic sugar to allow usage as a callable.
        """
        return self.run(inputs)


class HandlerFactory(ABC, Generic[InputT, OutputT]):
    """
    Abstract factory for creating handlers.

    Why Factories?
    1. Serialization: Handlers with state that can't be pickled.
        Factories are lightweight and picklable, allowing them to be sent to worker processes.
    2. Lifecycle Control: Allows the worker to control WHEN the heavy initialization happens.
    """

    @abstractmethod
    def create() -> BaseHandler[InputT, OutputT]:
        """Create and return a new handler instance."""
        pass
