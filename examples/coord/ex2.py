import time

from pydantic import BaseModel

from tadween_core.broker import InMemoryBroker
from tadween_core.coord import StageContextConfig, WorkflowContext
from tadween_core.handler import BaseHandler
from tadween_core.workflow import Workflow

# 1. Define a "Stash" limitation logic
# We want to block the 'ingestion' stage if the 'stash' has more than 2 items.
STASH_LIMIT = 2


def is_stash_full(ctx: WorkflowContext, metadata: dict) -> bool:  # noqa: ARG001
    count = ctx.state_get("stash_count", 0)
    return count >= STASH_LIMIT


def increment_stash(ctx: WorkflowContext, metadata: dict):
    ctx.increment("stash_count", 1)
    print(
        f"  [STASH] Item {metadata.get('id')} added to stash. Total: {ctx.state_get('stash_count')}"
    )


def decrement_stash(ctx: WorkflowContext, metadata: dict):
    ctx.decrement("stash_count", 1)
    print(
        f"  [STASH] Item {metadata.get('id')} removed from stash. Total: {ctx.state_get('stash_count')}"
    )


# 2. Handlers
class SimpleInput(BaseModel):
    id: int


class IngestionHandler(BaseHandler[SimpleInput, SimpleInput]):
    def run(self, inputs: SimpleInput) -> SimpleInput:
        # Ingestion is very fast, just a gateway
        return inputs


class ProcessingHandler(BaseHandler[SimpleInput, SimpleInput]):
    def run(self, inputs: SimpleInput) -> SimpleInput:
        # Processing is slow, keeping items in the "stash" longer
        print(f"  [Processing] Working on item {inputs.id}...")
        time.sleep(1.0)
        print(f"  [Processing] Done with item {inputs.id}")
        return inputs


def main():
    print(f"Goal: Block ingestion when stash count >= {STASH_LIMIT}")

    broker = InMemoryBroker()
    context = WorkflowContext()

    # We use a workflow with default_payload_extractor=lambda x: None
    # to ensure the input payload is passed between stages.
    wf = Workflow(
        broker=broker, context=context, default_payload_extractor=lambda x: None
    )

    # 3. Configure Ingestion Stage
    # It will block using 'wait_for' if the stash is full.
    ingestion_config = StageContextConfig(
        context=context,
        defer_predicate=is_stash_full,
        defer_event="stash_cleared",
        defer_poll_interval=0.1,
        defer_state_update=increment_stash,
    )

    wf.add_stage("ingestion", IngestionHandler(), context_config=ingestion_config)

    # 4. Configure Processing Stage
    # It notifies the 'stash_cleared' channel when a task is finished.
    processing_config = StageContextConfig(
        context=context,
        done_state_update=decrement_stash,
        notify_events=["stash_cleared"],
    )

    wf.add_stage("processing", ProcessingHandler(), context_config=processing_config)

    wf.link("ingestion", "processing")
    wf.set_entry_point("ingestion")
    wf.build()

    # 5. Submit items
    print("[Main] Submitting 5 items...")
    for i in range(1, 6):
        wf.submit({"id": i}, metadata={"id": i})

    # Join the broker to wait for all messages to be processed
    # We should see Item 1 and 2 pass through, then 3, 4, 5 block until 1 and 2 finish.
    broker.join(timeout=15)

    wf.close(force=True)
    print("\n[Main] All items processed. Ingestion was successfully throttled.")


if __name__ == "__main__":
    main()
