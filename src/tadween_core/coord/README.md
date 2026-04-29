# Coord

The `coord` package provides low-level primitives for flow control, logical synchronization, and physical resource management.

These primitives sit between the core building blocks (`Broker`, `Cache`, `Repo`) and the higher-level orchestrators (`Stage`, `Workflow`), providing the "glue" that allows decoupled components to communicate their availability and constraints.

## Coordination vs. Orchestration

| Feature | Coordination (`coord`) | Orchestration (`workflow`) |
|---------|------------------------------|----------------------------|
| **Scope** | Local/Resource-specific | Global/DAG-wide |
| **Concern** | "Can I run now?" (Backpressure) | "What runs next?" (Topology) |
| **Mechanism** | Semaphores, Conditions, Predicates | Topics, Links, Routing Policies |
| **Knowledge** | Blind to stages and payloads | Aware of stage links and routing |

## Core Primitives

### ResourceManager (Physical Throttling)
Controls access to finite hardware or system resources (e.g., CUDA tokens, RAM MB).
- **Behavior**: Quantitatively tracks available units.
- **Blocking**: Blocks the caller if demands exceed current capacity.
- **Scope**: Typically shared across multiple stages to prevent resource exhaustion.

### WorkflowContext (Logical Synchronization)
Provides an event-based signaling bus for inter-stage coordination.
- **Behavior**: Hybrid notification/polling via `wait_for` predicates with atomic state transitions.
- **Channels**: Directed event channels to avoid "thundering herd" wake-ups.
- **Artifact Tracking**: Built-in support for distributed task tracking via `track_artifact_progress`.
- **Deadlock Prevention**: Dispatches notifications outside of internal locks.
- **Blocking**: Blocks the caller until a logical condition is met.
- **Contextual Hooks**: Supports passing arbitrary `metadata` to predicates and state update hooks.

---

## Key Patterns

### Artifact Quiescence (Completion Tracking)
In a distributed DAG, determining when an "Artifact" (a logical unit of work, e.g., a file) is fully processed is difficult. `WorkflowContext` provides a specialized method for tracking this logic without leaking memory or causing deadlocks.

**Resilience**: The tracking mechanism is built into the routing layer. An artifact is considered "done" when all its associated tasks (and any child tasks they spawned) have finished, **regardless of whether they succeeded or failed.**

```python
# In the entry point:
context.track_artifact_progress(artifact_id, 1)

# In a callback listener:
def cleanup_resources(artifact_id, **kwargs):
    # This runs exactly once when the entire chain for this ID is finished (Success OR Error)
    print(f"Artifact {artifact_id} lifecycle complete.")

context.on_artifact_done(cleanup_resources)
```

### Pure Predicates & Atomic Transitions
To avoid race conditions and side-effects during polling, predicates should be **pure functions**.
- **The Predicate**: Should only read state. Signature: `(WorkflowContext, dict) -> bool`.
- **The Transition**: State mutations should be defined via `update_on_acquire` (in `wait_for`) or `apply_state`. Signature: `(WorkflowContext, dict) -> None`. These are applied **atomically** while the internal lock is held.

### Event Notification
Notifications trigger registered callbacks.
- **Callback Signature**: `callback(**kwargs)`. The event name is omitted to keep handlers focused on domain data.
- **Exception Safety**: The context catches exceptions in callbacks, ensuring one failing handler doesn't crash the entire notification bus.

### Contextual Resource Tracking (Reference Counting)
In complex DAGs where an artifact (e.g., a large audio file) is used across multiple concurrent stages, simple global counters are insufficient. Contextual hooks allow tracking resource lifecycle by ID:

```python
# can_claim <=> defer_predicate
def can_claim(ctx: WorkflowContext, meta: dict) -> bool:
    # Block if limit reached AND this specific ID isn't already claimed
    return (ctx.state_get("active_count") >= 5 
            and meta["id"] not in ctx.state.get("active_ids", set()))

# claim_hook <=> update_on_acquire (the callable one)
def claim_hook(ctx: WorkflowContext, meta: dict) -> None:
    active_ids = ctx.state.setdefault("active_ids", set())
    if meta["id"] not in active_ids:
        active_ids.add(meta["id"])
        ctx.increment("active_count")

# Usage in StageContextConfig
config = StageContextConfig(
    defer_predicate=can_claim,
    defer_state_update=claim_hook,
    # ...
)
```

### Rollback Safety
The `Stage` collector loop ensures that logical claims are rolled back if subsequent steps (like physical resource acquisition or task submission) fail. This prevents "leaking" state increments if a message is never actually processed.

---

## Task Lifecycle & Flow Control

The following diagram illustrates the exact order of operations when a message enters a `Stage`. Flow control happens inside the **Collector Thread**, ensuring that logical and physical backpressure propagate upstream.

```
submit_message → [stage queue] 
                       ↓
                Collector Thread (DRAIN LOOP)
                       ↓
[STEP 1] Wait for Logical Predicate (coord.WorkflowContext)
         1. Block while predicate is True.
         2. Apply `defer_state_update` atomically when predicate is False (claim).
                       ↓
[STEP 2] Acquire Physical Resources (coord.ResourceManager)
         blocks until: {"cuda": 1, "RAM": 512} available
         (Failure here triggers rollback of Step 1 state)
                       ↓
[STEP 3] Stage Processing (_process_message)
         1. Intercept (policy.intercept)
         2. Resolve Inputs (policy.resolve_inputs)
         3. Validate (Input Model)
         (Failure here triggers rollback of Step 1 state and Step 2 release)
                       ↓
[STEP 4] Submit to Task Queue (Executor)
                       ↓
             WORKER EXECUTION (Handler.run)
                       ↓
[STEP 5] Task Completion Callback (_on_task_done)
         1. Release Physical Resources (ResourceManager.release)
         2. Notify Logical Channels (WorkflowContext.notify)
         3. Apply `done_state_update` (if configured)
         4. Policy Hooks (on_done, on_success/on_error)
```

## Anatomy

```text
└── coord
    ├── __init__.py
    ├── context.py   -> WorkflowContext & StageContextConfig
    └── resource.py  -> ResourceManager
```
