import time
from queue import Queue

from pydantic import BaseModel

from tadween_core.broker.memory import InMemoryBroker
from tadween_core.cache.simple_cache import SimpleCache
from tadween_core.coord import StageContextConfig
from tadween_core.coord.context import WorkflowContext
from tadween_core.handler.base import BaseHandler
from tadween_core.stage.policy import DefaultStagePolicy
from tadween_core.task_queue import init_queue
from tadween_core.workflow.workflow import Workflow

events = Queue()
MAX_STASH_DEPTH = 3


class ItemInput(BaseModel):
    item_id: int


class LargeItem(BaseModel):
    item_id: int
    item: str = "HUGE result"


class LoaderHandler(BaseHandler[ItemInput, LargeItem]):
    def run(self, inputs: ItemInput) -> LargeItem:
        return LargeItem(item_id=inputs.item_id)


class ConsumerHandler(BaseHandler[LargeItem, LargeItem]):
    def run(self, inputs):
        time.sleep(0.02)

        return LargeItem.model_construct(**inputs.__dict__)


class LoaderPolicy(DefaultStagePolicy[ItemInput, LargeItem, dict]):
    def on_success(self, task_id, message, result, broker=None, repo=None, cache=None):
        cache.set_bucket(message.metadata.get("cache_key"), {"item": result})
        events.put("+")


class ConsumerPolicy(DefaultStagePolicy[LargeItem, LargeItem, dict]):
    def on_success(self, task_id, message, result, broker=None, repo=None, cache=None):
        cache.delete_bucket(message.metadata.get("cache_key"))
        events.put("-")


def test_workflow_deferral_backpressure():
    broker = InMemoryBroker()
    cache = SimpleCache(schema_type=dict)

    def extractor(result):  # noqa: ARG001
        return None

    workflow = Workflow(broker=broker, cache=cache, default_payload_extractor=extractor)

    try:

        def defer_predicate(ctx: WorkflowContext):
            stash = ctx.state_get("stash_depth", 0)
            return stash >= MAX_STASH_DEPTH

        # Add Loader stage
        workflow.add_stage(
            name="Loader",
            handler=LoaderHandler(),
            task_queue=init_queue(max_workers=5),
            policy=LoaderPolicy(),
            context_config=StageContextConfig(
                defer_predicate=defer_predicate,
                defer_event="cache_stash",
                defer_timeout=5.0,
                defer_poll_interval=1,
                defer_state_update={"stash_depth": 1},
            ),
        )

        # Add Consumer stage
        workflow.add_stage(
            name="Consumer",
            handler=ConsumerHandler(),
            task_queue=init_queue(max_workers=1),
            policy=ConsumerPolicy(),
            context_config=StageContextConfig(
                notify_events=["cache_stash"],
                n_notify=1,
                done_state_update={"stash_depth": -1},
            ),
        )

        workflow.link("Loader", "Consumer")
        workflow.set_entry_point("Loader")
        workflow.build()

        # Submit 10 items rapidly
        for i in range(1, 11):
            workflow.submit(ItemInput(item_id=i), metadata={"cache_key": str(i)})

    finally:
        workflow.close()
        events.put_nowait(None)

    consecutive_stash = 0
    while True:
        item = events.get()
        if item is None:
            break

        if item == "+":
            consecutive_stash += 1
        else:
            consecutive_stash = 0
    # Adding to cache (stash) never exceeds the depth (logical backpressure)
    assert not consecutive_stash > MAX_STASH_DEPTH
