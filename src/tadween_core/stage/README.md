# Stage

A `Stage` is the domain layer of the pipeline. Where all other components (brokers, queues, caches, repos) are generic infrastructure, a Stage binds
them together around a specific piece of work.

A Stage is defined by four things:
- **Handler:** what computation this stage performs.
- **Policy:** what happens at each point in the stage's lifecycle.
- **Task Queue:** how that computation is executed (thread vs. process, worker count, initializer).
- **Stage Queue:** a bounded back-pressure buffer that decouples message submission from execution.

Optionally, a Stage can hold a `Cache`, `Repo`, and `Broker`.

---

## Internal Pipeline

```
submit_message → [stage queue] → collector thread → _process_message → task queue → callback
```

- **Stage queue** (`queue.Queue`): accepts messages via `submit_message`. When the queue is full, the caller blocks until a slot frees up. `queue_size=0` (default) means unbounded — no back-pressure.
- **Collector thread**: a daemon thread that continuously pulls messages from the stage queue and runs `_process_message`.
- **Task registry**: maps `message.id` → `task_id` so callers can resolve a submission ID to the underlying worker ID via `get_worker_task_id`.

---

## Submission APIs

| Method | Description |
|---|---|
| `submit_message(message, timeout=None)` | Submit a pre-built `Message`. Returns the message ID. Blocks if stage queue is full. |
| `submit(input_data, *, metadata=None, topic="N/A")` | Convenience: wraps raw input in a `Message` and delegates to `submit_message`. |

---

## Lifecycle Management

| Method | Description |
|---|---|
| `wait_all(timeout=None)` | Block until the stage queue is drained **and** all tasks in the task queue have completed. |
| `close(timeout=5, force=False)` | Shut down the stage. Unless `force`, drains queues gracefully first. |
| `get_worker_task_id(message_id)` | Resolve a message ID to its task-queue worker ID. |

## Stage Policy

The policy is the lifecycle contract of a stage. It defines *what happens when*
without coupling that logic to the handler or the executor.

### Control hooks (return values the stage acts on)

| Hook | Returns | Called when |
|---|---|---|
| `intercept(message, repo, cache)` | `InterceptionContext` | Message arrives. `intercepted=True` = policy handled it, stop workflow. `intercepted=False` = proceed normally. |
| `resolve_inputs(message, repo, cache)` | `InputT \| dict` | Normal workflow only. Determines where handler input comes from: payload, cache, or repo. |

When `intercept` returns `True`, the policy is fully responsible for what
happens next; it may call `on_success`, `on_error`, update metadata, or do
nothing at all. The stage will not proceed.

### Notification hooks (fire-and-forget)

| Hook | Fired when |
|---|---|
| `on_received(message)` | Message arrived at the stage. Always fires, before `intercept`. |
| `on_queued(task_id, message)` | Task successfully enqueued. Only fires on normal workflow. |
| `on_running(task_id, message)` | Task begins execution in the worker. |
| `on_done(message, envelope)` | Task finished, regardless of outcome. |
| `on_success(task_id, message, result, ...)` | Handler completed successfully. Also called by the policy itself from inside `intercept` when short-circuiting with a result. |
| `on_error(message, error, ...)` | Any failure in the stage lifecycle. The error type distinguishes origin: `HandlerError` for task-level failures, `InputValidationError` / `PolicyError` / `StageError` for stage-level failures. |

### Lifecycle order
```
submit_message → stage queue → collector thread
    on_received
    └── intercept → True:  policy owns everything from here
    └── intercept → False:
        resolve_inputs
        validate → task queue → on_queued
        on_running
        on_done
        on_success | on_error
```

#### Interception gotchas

- Once `InterceptionContext.intercepted` is `True`, you need to differentiate between:
  1. Workflow stage policy: This policy is wrapped by the workflow router. Workflow router publishes messages for fan-out topics, auto-acks our message _on success_ and _on interception_ and auto-negative-ack on failure. So `intercept` is straightforward, you don't need to maintain any interception implications logic.
  2. Standalone or manually connected stages: You define the whole logic once intercepted.
- `intercept` runs synchronously on the main thread at submission time. while `on_success` runs asynchronously after the worker completes. When you submit all tasks in rapid succession, every `intercept` call hits an empty cache because no task has had time to finish and write to it.<br>
If you use single stage and try to hit the cache, you would typically need to distribute submissions on intervals.

>Note: Default workflow router implementation fires `on_done` automatically. ***Never ack, nack, invoke `on_done` in workflow managed stage policies*** unless you know what you do.

- When using `InterceptionAction` to gain full control over the workflow (e.g., via `InterceptionAction.cancel()` or custom flags), you take on the "mental burden" of ensuring your policy hooks are consistent.

**Know what you want:**
- If you **short-circuit** (`publish=True`) but don't provide a `payload` in the context, subsequent stages might receive empty inputs and fail.
- If you enable `on_success=True` during interception, ensure your `on_success` hook can handle the `payload` you provided (e.g., if it expects a specific result format to write to a cache).
- Always ensure `ack=True` is set unless you have a very specific reason to keep the message "in flight" or plan to handle the acknowledgement manually.
---

## Stage Policy Decorators

Policy decorators help reduce boilerplate for common patterns like caching, timing, and dependency injection.

### Data Injection
- `@inject_cache(cache_field, inject_as, cache_key="cache_key")`: Injects a value from the cache bucket into `resolve_inputs` keyword arguments.
- `@inject_repo(part, inject_as, aid="artifact_id")`: Injects an artifact part from the repository into `resolve_inputs` keyword arguments.

These can be stacked for fallback logic (e.g., check cache first, then repo):
```python
@inject_cache(cache_field="audio_array", inject_as="audio")
@inject_repo(part="audio", inject_as="audio")
def resolve_inputs(self, message, audio=None, **kwargs):
    # `audio` will be populated from cache if found, otherwise from repo.
    return MyInput(audio=audio)
```

### Lifecycle & Utilities
- `@write_cache(cache_field, result_field)`: Automatically writes handler results to cache on success.
- `@check_repo(aid, condition)`: Intercepts execution if certain artifact parts already exist in the repo.
- `@done_timing()`: Logs execution duration and waiting time.
- `@log_errors()`: Automatically logs errors encountered during the stage lifecycle.
- `@side_effect`: Marks a policy hook (like `on_success`) as non-critical. Exceptions will be caught, logged as warnings, and suppressed, allowing the pipeline to continue. Use this for webhooks, analytics, or other optional side-effects.

## Critical vs. Non-Critical Events

In `tadween-core`, the **Streamline Path** (Data Flow) is strictly reliable. By default, any exception in a policy hook is treated as a **Critical Failure**:

1. **Policy Hooks as Gatekeepers**: Hooks like `on_success` often handle data persistence or stashing that downstream stages depend on. If these fail, the pipeline *must* stop to prevent inconsistent data or "zombie" tasks.
2. **Error Propagation**: An unhandled exception in a hook is wrapped in a `PolicyError`, triggering `on_error` and stopping the message's propagation through the DAG.

### When to use `@side_effect`
Use the `@side_effect` decorator when a piece of logic is **informational** or **external** and should not affect the core data flow:
- Sending a notification or webhook.
- Updating an external dashboard.
- Writing to a non-essential log.

```python
class MyPolicy(DefaultStagePolicy):
    def on_success(self, task_id, message, result, ...):
        # 1. CRITICAL: Save to repo for next stage (if this fails, pipeline stops)
        repo.save(result)

        # 2. NON-CRITICAL: Side-effect (if this fails, pipeline continues)
        try:
            notify_external_service(result)
        except Exception as e:
            logger.warning(...)
```

## Anatomy
```
└── stage
    ├── __init__.py
    ├── policy.py      → Base contract, DefaultStagePolicy (no-op), StagePolicyBuilder.
    ├── decorators.py   → Policy decorators (cache, repo injection, timing, error logging).
    ├── stage.py        → Stage implementation.
    └── README.md
```

---

## Usage

*See [examples/stage/](../../../examples/stage/README.md) for runnable examples.*