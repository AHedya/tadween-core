# Workflow

The `workflow` package orchestrates `Stage` objects into a Directed Acyclic Graph (DAG). It manages topology, message routing between stages, and lifecycle.

## Concepts

### Workflow
the top-level orchestrator. Stages are registered, linked into a DAG (or linear pipeline),
and wired to a broker during `build()`. After that, submitting a payload to the entry
point topic triggers the full pipeline automatically.

Topology constraints enforced at link time:
- No fan-in: a stage may have at most one parent.
- No cycles: the graph must remain acyclic.

### WorkflowRoutingPolicy
A decorator that wraps any `StagePolicy` to add the
transport layer. The user's policy handles domain concerns (persistence, caching,
logging). The routing policy handles infrastructure concerns (ack/nack, publishing
to the next stage's topic). The two layers never need to know about each other.

### Coord & Flow Control
Tadween uses a dedicated `coord` layer for managing logical and physical backpressure. 

- **WorkflowContext**: A shared synchronization engine used for logical coordination and inter-stage signaling. It provides a hybrid notification/polling mechanism (`wait_for`) that allows stages to stall their processing until a logical predicate is met.
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
    2. publish to output topics  (routing: forward to next stage)
    3. broker.ack                (transport: mark message consumed)
5. WorkflowRoutingPolicy.on_error:
    1. inner policy on_error     (domain: log, alert)
    2. broker.nack               (transport: reject message)
6. Task Completion:
    1. **Release Resources**: Returns units to `ResourceManager`.
    2. **Notify Events**: Triggers `notify_events` channels to wake up deferred stages.


## Anatomy

    └── workflow
        ├── __init__.py
        ├── router.py   -> WorkflowRoutingPolicy. Wraps user policy with routing layer.
        ├── workflow.py -> Workflow. DAG builder, topology enforcement, lifecycle.
        └── README.md

Coordination primitives (`ResourceManager`, `WorkflowContext`) live in `tadween_core/coord/`.

## Usage

*See [examples/workflow/](../../../examples/workflow/README.md)*