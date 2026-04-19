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
- **Channels**: Directed event channels (e.g., "stash_updated") to avoid "thundering herd" wake-ups.
- **Blocking**: Blocks the caller until a logical condition is met.

---

## Key Patterns

### Pure Predicates & Atomic Transitions
To avoid race conditions and side-effects during polling (due to spurious wakeups), predicates should be **pure functions**.
- **The Predicate**: Should only read state (e.g., `lambda ctx: ctx.state_get("stash") >= 3`).
- **The Transition**: State mutations should be defined via `defer_state_update`. These are applied **atomically** only once the predicate clears and the internal lock is held.

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
