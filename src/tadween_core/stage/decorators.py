"""Policy decorators for reducing boilerplate in StagePolicy implementations.

This module provides method decorators and class decorators that automate
common patterns like caching, timing, and error logging.
"""

import functools
from collections.abc import Callable
from typing import Any, Literal, cast

from typing_extensions import ParamSpec

from tadween_core.broker import Message
from tadween_core.cache.cache import Cache
from tadween_core.task_queue.base import TaskEnvelope

P = ParamSpec("P")


def write_cache(
    cache_field: str | list[str],
    result_field: str | list[str] | None,
    cache_key: str = "cache_key",
    mode: str = "before",
) -> Callable[[Callable[P, None]], Callable[P, None]]:
    """
        Decorator for `on_success` stage policy event that writes result fields to cache.

        Maps data from the handler's result object to a cache bucket identified by
        message.metadata[cache_key]. Supports mapping single or multiple fields.

        Args:
            cache_field: The attribute name to write to on the cache bucket.
            result_field: The attribute name to read from on the result object. If set to `None`, whole result
                object is saved in cache field.
            cache_key: Metadata key for cache lookup. Defaults to "cache_key".
            mode: "before" to write before calling original method, "after" to write after.
                Defaults to "before".

    Raises:
            ValueError: If cache_field and result_field are lists of different lengths.

        Example:
            ```python
            # Single field mapping:
            @write_cache(cache_field="cached_audio", result_field="audio_array")
            def on_success(self, task_id, message, result, ...):
                pass

            # Multiple field mapping:
            @write_cache(
                cache_field=["cached_audio", "cached_meta"],
                result_field=["audio_array", "metadata"]
            )
            def on_success(self, task_id, message, result, ...):
                pass
            ```
    """
    cache_field = [cache_field] if isinstance(cache_field, str) else cache_field
    if result_field is None:
        result_field = [None]
    else:
        result_field = [result_field] if isinstance(result_field, str) else result_field
    if len(cache_field) != len(result_field):
        raise ValueError(
            f"Length mismatch: cache_field ({len(cache_field)}) and "
            f"result_field ({len(result_field)}) must have the same length."
        )

    def decorator(method: Callable[P, None]) -> Callable[P, None]:
        @functools.wraps(method)
        def wrapper(
            self: Any,
            task_id: str,
            message: Message,
            result: Any,
            broker: Any = None,
            repo: Any = None,
            cache: Cache[Any] | None = None,
        ) -> None:
            bucket_key = message.metadata.get(cache_key) if message.metadata else None

            def do_write() -> None:
                if bucket_key is not None and cache is not None:
                    bucket = cache.get_or_create(bucket_key)

                    for cf, rf in zip(cache_field, result_field, strict=True):
                        value = getattr(result, rf, None) if rf is not None else result

                        if value is not None:
                            setattr(bucket, cf, value)

            if mode == "before":
                do_write()
                method(self, task_id, message, result, broker, repo, cache)
            else:
                method(self, task_id, message, result, broker, repo, cache)
                do_write()

        return wrapper

    return decorator


def done_timing(
    stage_name: str | None = None,
    label_key: str = "id",
    mode: Literal["before", "after"] = "after",
    callback: Callable[[str, str, float, float], None] | None = None,
) -> Callable[[Callable[P, None]], Callable[P, None]]:
    """
    Decorator for `on_done` that logs execution timing.

    Logs timing information using the provided callback or falls back
    to printing to stdout. The timing is extracted from the TaskEnvelope.

    Args:
        stage_name: Name of the stage for logging. If None, uses the policy
            class name (self.__class__.__name__) at runtime. Defaults to None.
        label_key: Metadata key for labeling. Use logical id. (default: "id").
        mode: "before" to log before calling original method, "after" to log after.
            Defaults to "after".
        callback: Optional callback(stage_name, label, waiting, duration) for custom logging.
            If None, prints to stdout in format: "[stage_name] label: (waiting,duration)"

    Example:
        @log_timing()  # Uses class name as stage name
        def on_done(self, message, envelope):
            pass

        @log_timing(stage_name="AudioLoader", mode="before")
        def on_done(self, message, envelope):
            pass
    """

    def decorator(method: Callable[P, None]) -> Callable[P, None]:
        @functools.wraps(method)
        def wrapper(
            self: Any,
            message: Message,
            envelope: TaskEnvelope[Any],
        ) -> None:
            def do_log() -> None:
                resolved_name = (
                    stage_name if stage_name is not None else self.__class__.__name__
                )
                label = (
                    message.metadata.get(label_key, "N/A")
                    if message.metadata
                    else "N/A"
                )
                duration = envelope.metadata.duration
                waiting = envelope.metadata.waiting

                if callback is not None:
                    callback(resolved_name, cast(str, label), waiting, duration)
                else:
                    print(
                        f"[{resolved_name}] {label}: ({round(waiting, 3)},{round(duration, 3)})"
                    )

            if mode == "before":
                do_log()
                method(self, message, envelope)
            else:
                method(self, message, envelope)
                do_log()

        return wrapper

    return decorator


def log_errors(
    mode: str = "before",
    callback: Callable[[Message, Exception], None] | None = None,
) -> Callable[[Callable[P, None]], Callable[P, None]]:
    """
    Decorator for on_error that logs errors.

    Logs error information using the provided callback or falls back
    to printing to stdout.

    Args:
        mode: "before" to log before calling original method, "after" to log after.
            Defaults to "before" to ensure error is captured even if original raises.
        callback: Optional callback(message, error) for custom logging.
            If None, prints to stdout in format: "error: {error}"

    Example:
        @log_errors()
        def on_error(self, message, error, broker=None):
            pass  # Error logged automatically

        @log_errors(mode="after", callback=my_logger)
        def on_error(self, message, error, broker=None):
            pass
    """

    def decorator(method: Callable[P, None]) -> Callable[P, None]:
        @functools.wraps(method)
        def wrapper(
            self: Any,
            message: Message,
            error: Exception,
            broker: Any = None,
        ) -> None:
            def do_log() -> None:
                if callback is not None:
                    callback(message, error)
                else:
                    print(f"error: {message}, {error}")

            if mode == "before":
                do_log()
                method(self, message, error, broker)
            else:
                method(self, message, error, broker)
                do_log()

        return wrapper

    return decorator
