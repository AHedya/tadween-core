# Stage

A `Stage` is the domain layer of the pipeline. Where all other components (brokers, queues, caches, repos) are generic infrastructure, a Stage binds
them together around a specific piece of work.

A Stage is defined by three things:
- **Handler:** what computation this stage performs.
- **Policy:** what happens at each point in the stage's lifecycle.
- **Task Queue:** how that computation is executed (thread vs. process,
  worker count, initializer).

Optionally, a Stage can hold a `Cache`, `Repo`, and `Broker`.

---

## Stage Policy

The policy is the lifecycle contract of a stage. It defines *what happens when*
without coupling that logic to the handler or the executor.

### Control hooks (return values the stage acts on)

| Hook | Returns | Called when |
|---|---|---|
| `intercept(message, repo, cache)` | `bool` | Message arrives. `True` = policy handled it, stop workflow. `False` = proceed normally. |
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
on_received
└── intercept → True:  policy owns everything from here
└── intercept → False:
    resolve_inputs
    on_queued
    on_running
    on_done
    on_success | on_error
```

Important note: `intercept` runs synchronously on the main thread at submission time. while `on_success` runs asynchronously after the worker completes. When you submit all tasks in rapid succession, every `intercept` call hits an empty cache because no task has had time to finish and write to it.<br>
If you use single stage and try to hit the cache, you would typically need to distribute submissions on intervals.

## Anatomy
```
└── stage
    ├── __init__.py
    ├── policy.py   -> Base contract, DefaultStagePolicy (no-op), StagePolicyBuilder.
    ├── stage.py    -> Stage implementation.
    └── README.md
```

---

## Usage

*See [examples/stage/](../../../examples/stage/README.md) for runnable examples.*