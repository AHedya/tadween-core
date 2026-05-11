# Workflow

The `workflow` package orchestrates `Stage` objects into a Directed Acyclic Graph (DAG). It manages topology, message routing between stages, and lifecycle.

## Concepts

### Workflow
the top-level orchestrator. Stages are registered, linked into a DAG (or linear pipeline),
and wired to a broker during `build()`. After that, submitting a payload to the entry
point topic triggers the full pipeline automatically.

Topology constraints enforced at link time:
- No fan-in: a stage may have at most one parent. This is restricted explicitly because the routing layer currently lacks a built-in mechanism to distinguish between stream aggregation (waiting to join results from multiple parents before executing) and multiplexing (multiple parents feeding independent messages into a shared queue). For aggregation scenarios, consider leveraging `WorkflowContext` defer logic or artifact completion events.
- No cycles: the graph must remain acyclic.

### WorkflowRoutingPolicy
A decorator that wraps any `StagePolicy` to add the
transport layer. The user's policy handles domain concerns (persistence, caching,
logging). The routing policy handles infrastructure concerns (ack/nack, publishing
to the next stage's topic). The two layers never need to know about each other.

#### Payload Extraction & Propagation
The `payload_extractor` is a callable that determines what data is passed from the current stage to the next stage(s) in the DAG. It is applied to the stage's result (or the interception payload).

The return value of the extractor dictates the propagation behavior:

| Return Value | Semantic | Description / Use Case |
| :--- | :--- | :--- |
| `None` | **Full Propagation** | Propagates the **original message payload** as-is. Useful for "streamline" stages where multiple sequential stages consume the exact same input data. |
| `{}` | **Blocking Propagation** | Pass an **empty dictionary**. Recommended for **heavy outputs or fan-out** scenarios. This prevents overloading the broker and encourages downstream stages to fetch data from shared storage (Cache/Repo) using IDs from metadata. |
| `x` (the input) | **Result Propagation** | Passes the **stage result** (the `OutputT`) as the next payload. Best for standard pipelines where Stage A's output is Stage B's input. |
| `dict` | **Selective Propagation** | Returns a custom dictionary. Use this to **pluck specific fields** from a large result, excluding heavy data while still passing necessary context to the next stage. |

You can set a workflow-wide default via `default_payload_extractor` and override it per-stage in `add_stage` or `integrate_stage`.

### Coord & Flow Control
Tadween uses a dedicated `coord` layer for managing logical and physical backpressure. 

- **WorkflowContext**: A shared synchronization engine used for logical coordination and inter-stage signaling. It provides a hybrid notification/polling mechanism (`wait_for`) that allows stages to stall their processing until a logical condition is met. It also handles **Artifact Completion Tracking**, automatically notifying when all concurrent branches for a specific artifact ID have finished.

- **StageContextConfig**: A configuration object for stages to declare their coordination requirements (predicates, event channels, and notification targets).
- **ResourceManager**: Handles physical resource throttling (e.g. CUDA, RAM).

These primitives are imported from `tadween_core.coord` (or simply `tadween_core`).

Stages declare their synchronization requirements via `StageContextConfig`:
- `defer_predicate`: A callable that must return `True` before the stage proceeds.
- `defer_event`: The channel name the stage listens to for re-evaluating its predicate.
- `notify_events`: A list of channels to notify when the stage completes a task.

This mechanism is separate from physical throttling (Resource Management) and is ideal for scenarios like "don't load more audio until the inter-stage stash is below X items" or "We've hit ratelimit, let's _wait_ for a while".
Wrapping happens automatically inside `integrate_stage`. In other words, if you use workflow to manage stages, ***stages policies should never be routing-aware*** (ack/nack, publishing to the next stage's topic). 

## Lifecycle order (per stage, inside a workflow)

message arrives on broker topic
1. Stage gets a message (`stage.submit_message(message)`)
2. Collector Thread pulls message:
    1. **Wait for Defer Predicate**: Blocks if `defer_predicate` is False.
    2. **Acquire Resources**: Blocks until `ResourceManager` capacity is available.
3. normal Stage lifecycle: intercept, resolve_inputs, handler, on_done
4. WorkflowRoutingPolicy.on_success:
    1. inner policy on_success   (domain: save, cache, metrics)
    2. extract payload
    3. publish to output topics  (routing: forward to next stage)
    4. broker.ack                (transport: mark message consumed)
5. WorkflowRoutingPolicy.on_error:
    1. evaluate retry policies.
    2. inner policy on_error (ONLY on terminal failure)
    3. broker.nack (transport: reject message, optionally requeue if retrying)
6. Task Completion:
    1. **Release Resources**: Returns units to `ResourceManager`.
    2. **Notify Events**: Triggers `notify_events` channels to wake up deferred stages.


## Anatomy

    └── workflow
        ├── __init__.py
        ├── router.py   -> WorkflowRoutingPolicy. Wraps user policy with routing layer.
        ├── retry.py    -> dataclass and constants used for retry mechanism
        ├── workflow.py -> Workflow. DAG builder, topology enforcement, lifecycle.
        └── README.md

Coordination primitives (`ResourceManager`, `WorkflowContext`) live in `tadween_core/coord/`.

## Usage

*See [examples/workflow/](../../../examples/workflow/README.md)*