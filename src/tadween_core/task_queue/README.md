# Task Queue

The `task_queue` package provides a robust interface for asynchronous task execution in Tadween.

## Concepts

- **BaseTaskQueue**: An abstract base class for all task queues (`submit`, `get_status`, `get_result`).
- **ThreadTaskQueue**: A lightweight queue using threads. Ideal for I/O-bound tasks.
- **ProcessTaskQueue**: A queue using process pools. Ideal for CPU/GPU-bound tasks.
- **BaseTaskPolicy**: A lifecycle object that defines hooks for task submission, running, and completion.

## Lifecycle Hooks

A `TaskQueuePolicy` defines what happens at each stage of a task's life:
- `on_submit`: Runs in the main process when a task is submitted.
- `on_running`: Runs in the worker process when a task starts running.
- `on_done`: Runs in the main process when a task finishes.

##  Usage Example

```python
from tadween_core.task_queue import ThreadTaskQueue

def my_heavy_task(x: int, y: int) -> int:
    return x + y

# 1. Initialize Task Queue
queue = ThreadTaskQueue(name="ComputeQueue", max_workers=4)

# 2. Submit a task
task_id = queue.submit(my_heavy_task, x=10, y=20)

# 3. Check status
status = queue.get_status(task_id)

# 4. Get result (blocking)
result = queue.get_result(task_id, timeout=5.0)
print(result)  # 30

# 5. Cleanup
queue.close()
```

##  Streaming Results

The task queue supports streaming completed tasks as they finish:

```python
for task_id, result in queue.stream_completed():
    print(f"Task {task_id} finished with result: {result}")
```

---
*For more examples, see [examples/task_queue.py](../../../examples/task_queue.py) (if available).*
