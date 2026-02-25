from typing import Any


class TadweenError(Exception):
    """Base exception for all Tadween-core errors."""

    def __init__(self, message: str, **context: Any):
        self.message = message
        self.context = context
        super().__init__(self.message)

    def __str__(self) -> str:
        ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items() if v is not None)
        if ctx_str:
            return f"{self.message} ({ctx_str})"
        return self.message


class StageError(TadweenError):
    """Errors occurring within a Stage."""

    def __init__(self, message: str, stage_name: str, **context: Any):
        super().__init__(message, stage_name=stage_name, **context)


class PolicyError(StageError):
    """Errors occurring during policy execution."""

    def __init__(
        self, message: str, stage_name: str, policy_name: str, method: str, **context: Any
    ):
        super().__init__(
            message,
            stage_name=stage_name,
            policy_name=policy_name,
            method=method,
            **context,
        )


class InputValidationError(StageError):
    """Errors occurring during input validation/conversion."""

    def __init__(self, message: str, stage_name: str, **context: Any):
        super().__init__(message, stage_name=stage_name, **context)


class HandlerError(StageError):
    """Errors occurring during handler execution."""

    def __init__(self, message: str, stage_name: str, **context: Any):
        super().__init__(message, stage_name=stage_name, **context)


class RoutingError(StageError):
    """Errors occurring during message routing between stages."""

    def __init__(self, message: str, stage_name: str, **context: Any):
        super().__init__(message, stage_name=stage_name, **context)
