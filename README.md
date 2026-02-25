# Tadween-core

The heart of the Tadween ecosystem. A modular, composable framework for building complex, stateful processing pipelines — from simple linear chains to full DAG-based workflows

## Quick Start

Tadween-core provides the orchestration logic and contracts for any asynchronous, stateful workflow.

```python
from tadween_core.workflow import Workflow
from tadween_core.broker import InMemoryBroker

# 1. Initialize core components
broker = InMemoryBroker()
workflow = Workflow(broker=broker, name="MyCustomPipeline")

# 2. Add stages (Handlers + Policies)
workflow.add_stage("ingest", IngestHandler())
workflow.add_stage("process", AIHandler())

# 3. Link them into a DAG
workflow.link("ingest", "process")
workflow.set_entry_point("ingest")

# 4. Build and Run
workflow.build()
workflow.submit({"data": "source_uri"})
```

Explore the [examples/](examples/) directory for complete, runnable use cases.

## Core Philosophy

Tadween-core is built on the principle that every step of a pipeline should be optional, swappable, and "declarative." It provides the **contracts** and **orchestration**, while specific providers implement the computational logic.

- **Composable**: Build complex DAGs from simple, reusable stages.
- **Environment Agnostic**: Run locally, in production, or distributed.
- **Type-Safe**: Leveraging Pydantic for robust I/O and state management.
- **Stateful**: Integrated caching and persistence layers.
- **Concurrency-First**: Managed thread and process-based task queues resolve I/O and CPU/GPU bottlenecks by allowing stages to run in parallel where the DAG permits.

## System Architecture

The framework is composed of several independent but integrated modules:

| Component | Description | Documentation |
| :--- | :--- | :--- |
| **Handler** | The unit of execution (Computational or Operational). | [docs](src/tadween_core/handler/README.md) |
| **Task Queue** | Manages async execution (Threads or Processes). | [docs](src/tadween_core/task_queue/README.md) |
| **Workflow & Stage** | Orchestrates the DAG and manages stage-level logic. | [docs](src/tadween_core/workflow/README.md) |
| **Broker** | The message bus for inter-stage communication. | [docs](src/tadween_core/broker/README.md) |
| **Cache** | High-performance, type-safe caching system. | [docs](src/tadween_core/cache/README.md) |
| **Repository** | Persistence layer for Artifacts. | [docs](src/tadween_core/repo/README.md) |
| **Artifact** | The core data model moved across the pipeline. | [docs](src/tadween_core/types/artifact/README.md) |

## Installation & Tests

Ensure you have `uv` installed.

```bash
# Run tests
uv run pytest

# Verbose output
uv run pytest -v -s
```

---
*For detailed implementation details, please refer to the specific component documentation linked above.*

## Batteries Included: The Tadween Preset
While tadween-core is generic, it provides a high-level "Audio-to-Text" implementation out of the box.

## The Backstory
A note from the developer.

`Tadween` began as a simple, monolithic end-to-end audio-to-text pipeline. The initial prototype was full of hardcoded bits, no clear standards, and no normalization — so any change felt like walking through a minefield. Building it revealed what a solid system really needs, especially when it must run in different environments: local machines, production servers, serverless platforms, or even distributed setups with many GPUs.

Early pipeline design was strictly stage-based: an ASR stage, then an LLM stage. The ASR stage was one big function with dozens of flags: should we run ASR only? ASR + alignment? ASR + alignment + diarization? The output was always the same Pydantic model, with many optional fields set to `None`. That result then needed to be normalized and cleaned of potential ASR hallucination artifacts before being fed into the LLM, which handled context reconstruction (audio is noisy and ASR quality isn't always perfect), insight extraction, and Q&A.
Because ASR and LLM work are expensive, we introduced an artifact model to track progress and let us retry or resume from where we left off.

The linear nature of the pipeline stages forced a choice: either pass the whole batch through each stage sequentially, or iterate over each artifact and process it one by one — also sequentially. Imagine a pipeline that first needs to download files (I/O) and then decompress them (CPU). In a linear setup you’d wait for all downloads to finish before starting decompression — slow and wasteful unless you write custom code to run I/O and CPU tasks concurrently.
Or imagine swapping out a diarization component: swapping `pyannote` for `nvidia-toolkit` should be simple, but it wasn’t.

Storage choices had the same friction. Do we save artifacts to the filesystem? That’s fine locally, but serverless often needs S3. In production you might prefer a database. These environment differences made it clear we needed something more flexible.


We decided to stop patching the pipeline and start building the engine we actually needed. We spent months "dividing and conquering" these pain points:
- We turned the monolithic pipeline into a _Generic_ `Workflow Builder`.
- We abstracted the messaging into `Brokers` and the execution into `Task Queues`.
- We abstracted the domain need by `Stage`.
- We abstracted persistence layer into `Repo`s. With built-in implementation for lazy-loading
- We redesigned the sequential, dependent I/O stream into `StagePolicy.resolve_inputs` in addition to typed caching system.  
- We redesigned the Artifact to be lazy-loaded and state-aware, moving from a single "God Object" to a modular entity with persistent "Parts."