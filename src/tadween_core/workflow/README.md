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

Wrapping happens automatically inside `integrate_stage`. In other words, if you use workflow to manage stages, ***stages policies should never be routing-aware*** (ack/nack, publishing to the next stage's topic). 

## Lifecycle order (per stage, inside a workflow)

message arrives on broker topic
1. Stage gets a message (`stage.submit_message(message)`)
2. normal Stage lifecycle: intercept, resolve_inputs, handler, on_done
3. WorkflowRoutingPolicy.on_success:
    1. inner policy on_success   (domain: save, cache, metrics)
    2. publish to output topics  (routing: forward to next stage)
    3. broker.ack                (transport: mark message consumed)
4. WorkflowRoutingPolicy.on_error:
    1. inner policy on_error     (domain: log, alert)
    2. broker.nack               (transport: reject message)

## Anatomy

    └── workflow
        ├── __init__.py
        ├── router.py   -> WorkflowRoutingPolicy. Wraps user policy with routing layer.
        ├── workflow.py -> Workflow. DAG builder, topology enforcement, lifecycle.
        └── README.md

## Usage

*See [examples/workflow/](../../../examples/workflow/README.md)*