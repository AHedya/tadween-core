# Task Queue

The `task_queue` package provides a robust interface for asynchronous task execution in Tadween.

## Concepts

- **BaseTaskQueue**: An abstract base class for all task queues (`submit`, `get_status`, `get_result`).
- **ThreadTaskQueue**: A lightweight queue using threads. Ideal for I/O-bound tasks.
- **ProcessTaskQueue**: A queue using process pools. Ideal for CPU-bound tasks.
- **BaseTaskPolicy**: A lifecycle object that defines hooks for task submission, running, and completion.

## Anatomy
`handler` subpackage anatomy:

```
└── task_queue
    ├── __init__.py         => `init_queue` interface
    ├── base_policy.py      => task queue policy contract
    ├── base_queue.py       => task queue contract. implements most of the core logic
    ├── base.py             => shared types
    ├── dynamic.py          => supports both thread and process pool executor
    ├── policy.py           => default no-op, and LoggingPolicy implementation
    ├── process_queue.py    => process task queue implementation
    ├── README.md
    └── thread_queue.py     => thread task queue implementation
```

## Lifecycle Hooks

A `TaskQueuePolicy` defines what happens at each stage of a task's life:
- `on_submit`: Runs in the main process when a task is submitted.
- `on_running`: Runs in the worker process when a task starts running. Must be picklable if process-based executor.
- `on_done`: Runs in the main process when a task finishes.

## Result Consumption
Task queue has internal task to result table to save execution result in enveloped task. Enveloping a task is a pattern to obtain timing metrics without the need for external tool.
However, to save our task queue memory usage from growing so large, we pop the result on done (result consumption).
You can control result consumption by setting the flag `retain_result` in either task queue constructor, or override default task queue behavior by setting `retain_result` in `submit` method.

Optimally, using a task queue in a workflow, `retain_result` is set to False (default). And stage must modify `on_done` to instruct how to deal with the result: save to repository, fire event, or save to `Cache`.

##  Usage Example

*For examples, see [examples/task_queue/](../../../examples/task_queue/README.md) (if available).*


