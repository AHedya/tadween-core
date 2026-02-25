# Handler

The `handler` subpackage provides the fundamental unit of execution in Tadween workflows.

## Concepts

- **BaseHandler**: An abstract base class for all computational or operational logic.
- **InputT/OutputT**: Pydantic models used to define the handler's input and output contracts.
- **HandlerFactory**: An abstract factory for creating handlers. Used for deferred initialization and picklable task submission.

## Anatomy
`handler` subpackage anatomy:

```
└── handler
    ├── __init__.py => package interface
    ├── base.py     => contracts: `BaseHandler` and factory
    ├── defaults.py => prebuilt, ready-to-use handlers implementation
    ├── dummy.py    => dummy (but useful) handlers implementation
    └── README.md
```

## Lifecycle

A handler's lifecycle consists of:
1. `__init__`: Lightweight configuration (do NOT load heavy resources like models or DB connections).
2. `warmup`: Explicitly load heavy resources (optional, recommended).
3. `run`: Execute the core logic. Lazy-loads resources if `warmup` was not called.
4. `shutdown`: Cleanup resources (close connections, free VRAM).

## Usage Example

```python
from pydantic import BaseModel
from tadween_core.handler import BaseHandler

# 1. Define Input and Output
class MyInput(BaseModel):
    data: str

class MyOutput(BaseModel):
    result: str

# 2. Implement Handler
class MyHandler(BaseHandler[MyInput, MyOutput]):
    def warmup(self):
        # Load heavy model here
        pass

    def run(self, inputs: MyInput) -> MyOutput:
        # Perform computation
        return MyOutput(result=inputs.data.upper())

    def shutdown(self):
        pass

# 3. Usage
handler = MyHandler()
handler.warmup()
output = handler.run(MyInput(data="hello world"))
print(output.result)  # HELLO WORLD
handler.shutdown()
```

## Why Factories?

Handlers often contain non-picklable resources (e.g., loaded GPU models, database connections). `HandlerFactory` allows:
- **Serialization**: Send a lightweight factory to a worker process instead of a heavy handler instance.
- **Lifecycle Control**: Allow the worker process to control *when* heavy initialization happens.

---
*For more examples, see [examples/handler/](../../../examples/handler/README.md)*