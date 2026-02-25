# Workflow & Stage

The `workflow` and `stage` packages provide the high-level orchestration logic for building and managing complex pipelines in Tadween.

## Concepts

- **Stage**: A building block of a `Workflow`. It contains a single `Handler`, a `StagePolicy`, and its own `TaskQueue`, `Repository`, and `Cache`.
- **Workflow**: An orchestrator for `Stage` objects. It builds a Directed Acyclic Graph (DAG) and manages message routing between stages.
- **StagePolicy**: A lifecycle object that determines stage-level behavior (e.g., whether to process, skip, or cancel a message).
- **WorkflowRoutingPolicy**: A specialized policy that wraps a user's `StagePolicy` to handle message transport between stages.

## Stage-Level Orchestration

A `Stage` conducts a mini-orchestration of a single `Handler`'s execution:
1. **Submit**: Check with the `StagePolicy` if the message should be processed.
2. **Resolve**: Resolve inputs (from `Cache`, `Payload`, or `Repository`).
3. **Execute**: Submit the task to the stage's `TaskQueue`.
4. **Callback**: Trigger `StagePolicy` success or failure hooks when the task is done.

## Usage Example

```python
from tadween_core.workflow import Workflow
from tadween_core.broker import InMemoryBroker
from my_handlers import DownloadHandler, WhisperHandler

# 1. Initialize core components
broker = InMemoryBroker()
workflow = Workflow(broker=broker, name="MyAudioPipeline")

# 2. Add and integrate stages
workflow.add_stage("download", DownloadHandler())
workflow.add_stage("transcribe", WhisperHandler())

# 3. Link stages into a DAG
workflow.link("download", "transcribe")
workflow.set_entry_point("download")

# 4. Build and submit
workflow.build()
msg_id = workflow.submit({"url": "https://example.com/audio.mp3"})

# 5. Cleanup
workflow.close(timeout=5.0)
```

## Visualizing the Workflow

Tadween workflows can be visualized using Graphviz or Mermaid:

```python
workflow.visualize("workflow.png", format="graphviz")
```

---
*For more examples, see [examples/workflow.py](../../../examples/workflow.py) (if available).*
