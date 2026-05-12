"""
The Data Sync (Caching & Rollback)
Focus: Interception (Cache Hits), Rollback on Defer/Terminal Failures.

Having a simplified ETL pipeline (actually TL only -> transform and load), our task is to transform data,
which is a heavy task (process-based), then load (save) it into external db.
You typically need to be efficient by asking if your transformed data is already *transformed* or not.
If so,resume the workflow (shortcircuit) as it is.
"""

import threading

from pydantic import BaseModel

from tadween_core.cache.simple_cache import SimpleCache
from tadween_core.coord import StageContextConfig
from tadween_core.handler.base import BaseHandler
from tadween_core.stage.policy import (
    DefaultStagePolicy,
    InterceptionAction,
    InterceptionContext,
    StagePolicyBuilder,
)
from tadween_core.task_queue import init_queue
from tadween_core.workflow.retry import RetryPolicy
from tadween_core.workflow.workflow import Workflow


class SyncRecord(BaseModel):
    id: str
    data: str
    hash: str


class SyncResult(BaseModel):
    id: str
    status: str


class TransformerHandler(BaseHandler[SyncRecord, SyncResult]):
    def run(self, inputs: SyncRecord) -> SyncResult:
        return SyncResult(id=inputs.id, status="normalized")


transformer_lock = threading.Lock()
transformer_calls = 0


class LoaderHandler(BaseHandler[SyncResult, SyncResult]):
    def __init__(self):
        # it's safe to have a shared state here as this handler is thread-based.
        self._lock = threading.Lock()
        self.calls = 0

    def run(self, inputs: SyncResult) -> SyncResult:
        with self._lock:
            self.calls += 1
            raise ConnectionError("DatabaseConnectionLost")


loader_lock = threading.Lock()
loader_errors = 0
# ################### use extractor to pluck loader


class LoaderPolicy(DefaultStagePolicy):
    def on_error(self, message, error, broker=None):
        global loader_errors
        with loader_lock:
            loader_errors += 1


def check_cache_intercept(message, broker, repo, cache):  # noqa: ARG001
    payload = message.payload
    record_hash = payload.get("hash")
    if cache and record_hash in cache:
        return InterceptionContext(
            intercepted=True,
            # custom action. transformer is process based, so its handler can't have a state.
            # we will utilize `on_done` for marking true work being done. In other words, increase
            # our counter only if the transformer has actually run, and never increment in case of cache-hit
            action=InterceptionAction(
                on_done=False,
                on_success=True,
                publish=True,
                ack=True,
            ),
            # inject *transformed data*
            payload=SyncResult(id=payload["id"], status=cache[record_hash]["status"]),
            reason="Cache hit: record already transformed, needs to be loaded.",
        )


def save_to_cache_success(task_id, message, result, broker, repo, cache):  # noqa: ARG001
    payload = message.payload
    record_hash = payload.get("hash")
    if cache:
        # save arbitrary data just to mark it
        cache.set_bucket(record_hash, {"status": "synced"})


def inc_transformer(msg, env):  # noqa: ARG001
    global transformer_calls
    # update state
    with transformer_lock:
        transformer_calls += 1


def test_resilient_sync(inmemory_broker):

    cache = SimpleCache(schema_type=dict)
    workflow = Workflow(
        broker=inmemory_broker,
        cache=cache,
        default_payload_extractor=lambda x: x,
    )
    workflow.context.state["active_db_connections"] = 0

    # 1. Setup the Transformer with Caching Policy
    transformer = TransformerHandler()
    loader = LoaderHandler()

    transformer_policy = (
        StagePolicyBuilder()
        .with_intercept(check_cache_intercept)
        .with_on_success(save_to_cache_success)
        .with_on_done(inc_transformer)
    )

    workflow.add_stage(
        "transformer",
        handler=transformer,
        policy=transformer_policy,
        task_queue=init_queue("process", max_workers=2),
        context_config=StageContextConfig(
            predicate=lambda *_: False,
            on_acquire={"transformer_calls": 1},
        ),
    )

    workflow.add_stage(
        "loader",
        handler=loader,
        policy=LoaderPolicy(),
        context_config=StageContextConfig(
            predicate=lambda *_: False,
            on_acquire={"active_db_connections": 1},
            on_release={"active_db_connections": -1},
            event="loader_event",
        ),
        retry_policy=RetryPolicy(retry_on={ConnectionError}, max_retries=1),
        task_queue=init_queue("thread", max_workers=2),
    )

    workflow.link("transformer", "loader")
    workflow.set_entry_point("transformer")
    workflow.build()

    # Pre-populate cache for (5) records to simulate "already synced"
    for i in range(5):
        cache.set_bucket(f"hash-{i}", {"status": "synced"})

    # Execute 10 records
    for i in range(10):
        workflow.submit(
            SyncRecord(id=f"rec-{i}", data=f"data-{i}", hash=f"hash-{i}"),
        )
    # Wait for all stages to complete
    inmemory_broker.join(timeout=5)

    # (5) cache hits -> bypassed, so (5) handler calls
    assert transformer_calls == 5
    # context acquire only gets triggered if task is enqueued which didn't happen in cache hit scenarios
    assert workflow.context.state["transformer_calls"] == transformer_calls == 5

    # (10) calls went to loader, retried once each -> (20) calls
    assert loader.calls == 10 * 2  # fails and retires once again then terminal fail
    # on_error fires on terminal errors only (no retries left)
    assert loader_errors == 10

    # assert db connections never leak
    assert workflow.context.state["active_db_connections"] == 0

    print(loader_errors)
