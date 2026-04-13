# Logging

The `logger` subpackage provides three logger configuration interfaces for the `tadween` namespace.

## Loggers

| Logger | Handoff | Cross-process | Cleanup | Use case |
|--------|---------|---------------|---------|----------|
| **StandardLogger** | Direct handlers | No | None | General purpose, simple scripts |
| **QueueLogger** | Thread queue → listener | No | `close()` / `with` | High-throughput, latency-sensitive pipelines |
| **ProcessQueueLogger** | `multiprocessing.Queue` → listener | Yes | `close()` / `with` | Multi-process workflows |

### StandardLogger

Plug-and-play. Configures the `tadween` logger with direct handlers. No cleanup required.

```python
from tadween_core import StandardLogger
from tadween_core.logger import JsonFormatter

sl = StandardLogger(
    level=logging.DEBUG,
    log_path="app.log",
    file_formatter=JsonFormatter(),
)
logger = sl.logger
```

### QueueLogger

Thread-based queue. Closing-demanding — call `close()` or use as a context manager to flush buffered records. Forgetting to close risks losing a few buffered records only; the daemon listener won't block process exit.

```python
from tadween_core import QueueLogger

with QueueLogger(level=logging.DEBUG, log_path="app.log") as ql:
    ql.logger.info("non-blocking")
```

### ProcessQueueLogger

Process-safe queue using `multiprocessing.Queue`. Closing-demanding. Workers receive `pql.log_queue` and create their own `QueueHandler` after fork/spawn.

```python
from tadween_core import ProcessQueueLogger
import logging.handlers, multiprocessing

with ProcessQueueLogger(level=logging.DEBUG, log_path="app.log") as pql:
    pql.logger.info("main process log")

    # In worker processes:
    def worker(log_queue):
        qh = logging.handlers.QueueHandler(log_queue)
        logger = logging.getLogger("tadween")
        logger.addHandler(qh)
        logger.info("from worker")

    multiprocessing.Process(target=worker, args=(pql.log_queue,)).start()
```

Pass `mp_context` to control the start method:

```python
ctx = multiprocessing.get_context("spawn")
pql = ProcessQueueLogger(mp_context=ctx)
```

`mp_context` and `log_queue` are mutually exclusive.

## Formatter Dependency Injection

All three loggers accept `console_formatter` and `file_formatter` parameters, replacing any hardcoded format flags.

| Parameter | Default | Applies to |
|-----------|---------|------------|
| `console_formatter` | `NoStackTraceFormatter(...)` (suppresses tracebacks) | All loggers |
| `file_formatter` | `logging.Formatter(...)` (full tracebacks) | All loggers (when `log_path` is set) |

```python
from tadween_core.logger import JsonFormatter

# JSON file output
StandardLogger(log_path="app.json", file_formatter=JsonFormatter())

# Custom console format
StandardLogger(console_formatter=logging.Formatter("%(name)s: %(message)s"))
```

## Formatters

- **NoStackTraceFormatter**: Suppresses exception tracebacks in console output, keeping terminal logs clean.
- **JsonFormatter**: Outputs structured JSON log lines (timestamp, level, logger, message, optional exc_info/stack_info).

## Anatomy

```
└── logger
    ├── __init__.py           => re-exports public API
    ├── formatters.py         => NoStackTraceFormatter, JsonFormatter
    ├── simple.py             => StandardLogger class, set_logger function
    ├── queue.py              => QueueLogger class (thread-based)
    ├── process_queue.py      => ProcessQueueLogger class (process-safe)
    └── README.md
```

## Backward Compatibility

`set_logger()` is preserved as a convenience function that delegates to `StandardLogger`:

```python
from tadween_core import set_logger
logger = set_logger(level=logging.INFO, log_path="app.log")
```

## Custom Logger Integration

If you want to avoid interacting with the library's logger hierarchy:

- Use a logger with a **different name** (e.g., `"my_custom_logger"`)
- Disable propagation by setting `logger.propagate = False` (make sure to attach your own handlers)