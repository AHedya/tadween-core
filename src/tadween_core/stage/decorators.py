"""Policy decorators for reducing boilerplate in StagePolicy implementations.

This module provides method decorators and class decorators that automate
common patterns like caching, timing, and error logging.
"""

import functools
import logging
import re
from collections.abc import Callable
from typing import Any, Literal, cast

from typing_extensions import ParamSpec

from tadween_core.broker import Message
from tadween_core.cache.base import BaseCache
from tadween_core.repo.base import BaseArtifactRepo
from tadween_core.stage.policy import InterceptionContext
from tadween_core.task_queue.base import TaskEnvelope

P = ParamSpec("P")

logger = logging.getLogger("tadween.stage.decorators")


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
            cache: BaseCache[Any] | None = None,
        ) -> None:
            bucket_key = message.metadata.get(cache_key) if message.metadata else None

            def do_write() -> None:
                # TODO: determine whether a warning message if there's no bucket or there's no cache is enough, or fail loudly
                if bucket_key is not None and cache is not None and result is not None:
                    with cache.lock:
                        bucket = cache.get_bucket(bucket_key)

                        if bucket is None:
                            values = {}
                            for cf, rf in zip(cache_field, result_field, strict=True):
                                value = (
                                    getattr(result, rf, None)
                                    if rf is not None
                                    else result
                                )
                                if value is not None:
                                    values[cf] = value

                            bucket = cache.schema_type(**values)
                            cache.set_bucket(bucket_key, bucket)
                        else:
                            for cf, rf in zip(cache_field, result_field, strict=True):
                                value = (
                                    getattr(result, rf, None)
                                    if rf is not None
                                    else result
                                )
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
                    logger.info(
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
    log_exc_info: bool = True,
) -> Callable[[Callable[P, None]], Callable[P, None]]:
    """
    Decorator for on_error that logs errors.

    Logs error information using the provided callback or falls back
    to the module logger.

    Args:
        mode: "before" to log before calling original method, "after" to log after.
            Defaults to "before" to ensure error is captured even if original raises.
        callback: Optional callback(message, error) for custom logging.
            If None, logs to 'tadween.stage.decorators' logger.
        log_exc_info: Whether to include exception traceback in logs.
            Defaults to False.

    Example:
        @log_errors()
        def on_error(self, message, error, broker=None):
            pass  # Error logged automatically

        @log_errors(log_exc_info=True)
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
                    logger.error(
                        f"Error: {error}. Message: {message}",
                        exc_info=error if log_exc_info else False,
                    )

            if mode == "before":
                do_log()
                method(self, message, error, broker)
            else:
                method(self, message, error, broker)
                do_log()

        return wrapper

    return decorator


def check_repo(
    aid: str,
    condition: str,
) -> Callable[
    [Callable[P, InterceptionContext[Any]]], Callable[P, InterceptionContext[Any]]
]:
    """
    Decorator for `intercept` stage policy event that checks repo for artifact parts.

    Evaluates a condition string against the artifact loaded from the repo.
    Returns InterceptionContext(intercepted=True) if condition is met.
    Otherwise delegates to the original intercept method.

    Args:
        aid: Metadata key for the artifact ID.
        condition: String representing logic to check. Names are artifact fields
            (use dotted names for nesting). Supports '&' (and), '|' (or), and '()'.

    Example:
        ```python
        # Check if diarization part exists and has speakers field not None
        @check_repo(aid="artifact_id", condition="diarization.speakers")
        def intercept(self, message, broker=None, repo=None, cache=None):
            pass

        # Complex condition with multiple parts
        @check_repo(
            aid="audio_id",
            condition="audio | transcript & (meta.is_valid | valid)"
        )
        def intercept(self, message, broker=None, repo=None, cache=None):
            pass
        ```
    """
    # Extract identifiers (dotted paths) and operators
    tokens = re.findall(
        r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*|[&|()]", condition.replace(" ", "")
    )

    def decorator(
        method: Callable[P, InterceptionContext[Any]],
    ) -> Callable[P, InterceptionContext[Any]]:
        @functools.wraps(method)
        def wrapper(
            self: Any,
            message: Message,
            broker: Any = None,
            repo: BaseArtifactRepo | None = None,
            cache: BaseCache[Any] | None = None,
        ) -> InterceptionContext[Any]:
            if repo is None:
                return method(self, message, broker, repo, cache)

            artifact_id = message.metadata.get(aid) if message.metadata else None
            if not artifact_id:
                return method(self, message, broker, repo, cache)

            # Determine required parts
            valid_parts = set(repo._part_map.keys())
            parts_to_load = set()
            for token in tokens:
                if token not in ("&", "|", "(", ")"):
                    top_level = token.split(".")[0]
                    if top_level in valid_parts:
                        parts_to_load.add(top_level)

            # Load artifact
            try:
                artifact = repo.load(artifact_id, include=list(parts_to_load))
            except Exception:
                artifact = None

            # Evaluate condition
            expr_parts = []
            for token in tokens:
                if token in ("&", "|", "(", ")"):
                    expr_parts.append(token)
                else:
                    val = False
                    if artifact is not None:
                        obj = artifact
                        path_parts = token.split(".")
                        valid = True
                        for p in path_parts:
                            if not hasattr(obj, p):
                                valid = False
                                break
                            obj = getattr(obj, p)
                        if valid and obj is not None:
                            val = True
                    expr_parts.append("True" if val else "False")

            try:
                # Safe to eval since expr_parts only contains True, False, and operators
                is_intercepted = eval(" ".join(expr_parts), {"__builtins__": {}})
            except Exception:
                is_intercepted = False

            if is_intercepted:
                return InterceptionContext(intercepted=True)

            return method(self, message, broker, repo, cache)

        return wrapper

    return decorator


def side_effect(func: Callable[P, Any]) -> Callable[P, Any]:
    """
    Marks a policy hook as a non-critical side-effect.

    Exceptions raised by the decorated function will be caught, logged as
    warnings, and suppressed, allowing the stage's execution flow to continue.

    Example:
        @side_effect
        def on_success(self, result, ...):
            send_non_critical_webhook(result)
    """

    @functools.wraps(func)
    def wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> Any:
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            policy_logger = getattr(self, "logger", logger)
            policy_logger.warning(
                f"Non-critical side-effect failed in {func.__name__}: {e}.",
                exc_info=True,
            )
            return None

    return wrapper


def inject_cache(
    cache_field: str,
    inject_as: str,
    cache_key: str = "cache_key",
) -> Callable[[Callable[P, Any]], Callable[P, Any]]:
    """
    Decorator for `resolve_inputs` that injects a value from cache into kwargs.

    If `inject_as` is already in kwargs, it skips the cache lookup (allows stacking).
    Retrieves the cache key from message.metadata[cache_key].

    Args:
        cache_field: The attribute name to read from the cache bucket.
        inject_as: The keyword argument name to inject into the wrapped method.
        cache_key: Metadata key for cache lookup. Defaults to "cache_key".

    Example:
        ```python
        @inject_cache(cache_field="audio_array", inject_as="audio")
        def resolve_inputs(self, message, audio=None, **kwargs):
            return MyInput(audio=audio)
        ```
    """

    def decorator(method: Callable[P, Any]) -> Callable[P, Any]:
        @functools.wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Check if already injected by an outer decorator
            if inject_as in kwargs:
                return method(self, *args, **kwargs)

            if msg := kwargs.get("message"):
                message = msg
            elif args and isinstance(args[0], Message):
                message = args[0]
            else:
                return method(self, *args, **kwargs)

            cache = kwargs.get("cache")
            # args fallback
            if cache is None and len(args) > 2 and isinstance(args[2], BaseCache):
                cache = args[2]

            bucket_key = (
                message.metadata.get(cache_key)
                if message.metadata and cache_key
                else None
            )

            if bucket_key is not None and cache is not None:
                bucket = cache.get_bucket(bucket_key)
                if bucket is not None and hasattr(bucket, cache_field):
                    val = getattr(bucket, cache_field)
                    if val is not None:
                        kwargs[inject_as] = val

            return method(self, *args, **kwargs)

        return wrapper

    return decorator


def inject_repo(
    part: str,
    inject_as: str,
    aid: str = "artifact_id",
) -> Callable[[Callable[P, Any]], Callable[P, Any]]:
    """
    Decorator for `resolve_inputs` that injects a part from repo into kwargs.

    If `inject_as` is already in kwargs, it skips the repo lookup (allows stacking).
    Retrieves the artifact ID from message.metadata[aid].

    Args:
        part: The part name to load from the repository.
        inject_as: The keyword argument name to inject into the wrapped method.
        aid: Metadata key for the artifact ID. Defaults to "artifact_id".

    Example:
        ```python
        # Stacked: Cache first, then Repo fallback
        @inject_cache(cache_field="audio_array", inject_as="audio")
        @inject_repo(part="audio", inject_as="audio")
        def resolve_inputs(self, message, audio=None, **kwargs):
            return MyInput(audio=audio)
        ```
    """

    def decorator(method: Callable[P, Any]) -> Callable[P, Any]:
        @functools.wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Check if already injected by an outer decorator (e.g. inject_cache)
            if inject_as in kwargs:
                return method(self, *args, **kwargs)

            # Try to find message in args or kwargs
            if msg := kwargs.get("message"):
                message = msg
            elif args and isinstance(args[0], Message):
                message = args[0]
            else:
                return method(self, *args, **kwargs)

            repo = kwargs.get("repo")
            # args fallback
            if repo is None and len(args) > 1 and isinstance(args[2], BaseArtifactRepo):
                repo = args[2]

            artifact_id = (
                message.metadata.get(aid) if message.metadata and aid else None
            )

            if artifact_id is not None and repo is not None:
                try:
                    val = repo.load_part(artifact_id, part)
                    if val is not None:
                        kwargs[inject_as] = val
                except (KeyError, ValueError):
                    pass

            return method(self, *args, **kwargs)

        return wrapper

    return decorator
