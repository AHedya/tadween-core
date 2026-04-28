# Coordination examples

- [example 1](./ex1.py): basic high-level `WorkflowContext` usage. How to use `track_artifact_progress` and `on_artifact_done` to track global lifecycle for logical units (artifacts) without coupling stages.
- [example 2](./ex2.py): **Logical Synchronization (Backpressure)**. Use `wait_for` predicates and event notifications to implement advanced flow control (e.g., "Don't process more until the stash is cleared").
- [example 3](./ex3.py): **Physical Throttling with ResourceManager**. Control access to finite hardware resources (e.g., CUDA tokens, RAM) across multiple stages to prevent system exhaustion.
