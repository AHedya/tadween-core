"""
The Rate-Limited API Harvester (Sliding Window)
Focus:
- Logical Backpressure (Defer Logic), State Updates via `on_acquire`.
- Retry mechanism
- The handler is kept completely pure with *potential* failure mimicking connection errors.

"""

import time
from itertools import islice

from pydantic import BaseModel

from tadween_core.coord import StageContextConfig
from tadween_core.coord.context import WorkflowContext
from tadween_core.handler.base import BaseHandler
from tadween_core.task_queue import init_queue
from tadween_core.workflow.retry import RetryPolicy  # noqa
from tadween_core.workflow.workflow import Workflow

# Apply `LIMIT` during `DURATION`
DURATION = 0.05
LIMIT = 3
# random ids chosen to fail on
FAIL_ON = ["2", "0"]
RETIRES = 2

N_URLS = 20


class URLInput(BaseModel):
    url: str
    id: str | None = None


class URLOutput(BaseModel):
    url: str
    status: str


class APIFetcherHandler(BaseHandler[URLInput, URLOutput]):
    def run(self, inputs: URLInput) -> URLOutput:

        if inputs.id is not None and inputs.id in FAIL_ON:
            # arbitrary error with arbitrary error rate (fixed)
            raise RuntimeError
        time.sleep(0.01)
        return URLOutput(url=inputs.url, status="Success")


def detect_rate_exceeded(
    timestamps: list[float],
    duration=DURATION,
    limit=LIMIT,
) -> bool:
    if len(timestamps) <= limit:
        return False

    for window_start, current in zip(
        timestamps,
        islice(timestamps, limit, None),
        strict=False,
    ):
        if current - window_start < duration:
            print(current, window_start)
            return True
    return False


# our predicate
def check_rate_limit(ctx: WorkflowContext, metadata: dict):  # noqa: ARG001
    timestamps = ctx.state.get("request_timestamps")
    len_condition = len(timestamps) < LIMIT
    now = time.monotonic()

    if len_condition:
        return False

    should_halt = now - timestamps[-LIMIT] < DURATION

    return should_halt


# for updating the shared state
def request_done(ctx: WorkflowContext, metadata: dict):  # noqa: ARG001
    with ctx._lock:
        stamps = ctx.state.get("request_timestamps")
        now = time.monotonic()
        stamps.append(now)


def test_sliding_window_rate_limiter_succeeds(inmemory_broker):
    """Test how applying *rate-limit* logic by utilizing defer (logical backpressure) strictly comply with our rate-limit"""

    workflow = Workflow(
        broker=inmemory_broker,
    )
    timestamps = []
    workflow.context.state["request_timestamps"] = timestamps
    workflow.context.state["alt"] = []

    fetcher = APIFetcherHandler()

    workflow.add_stage(
        "fetcher",
        handler=fetcher,
        context_config=StageContextConfig(
            predicate=check_rate_limit,
            on_acquire=request_done,
            event="rate-limiter",
            poll_interval=0.05,  # Fallback to prevent deadlocks
        ),
        retry_policy=RetryPolicy(retry_on={RuntimeError}, max_retries=RETIRES),
        task_queue=init_queue("thread", max_workers=4),
    )
    workflow.set_entry_point("fetcher")
    workflow.build()

    # Overwhelm the pipeline with 10 URLs
    for i in range(N_URLS):
        workflow.submit(
            URLInput(url=f"http://api.com/data/{i}", id=str(i)),
        )

    # Wait for all stages to settle
    inmemory_broker.join(timeout=5)
    # our limiter works
    assert not detect_rate_exceeded(timestamps)
    # retry mechanism also works
    assert len(timestamps) == N_URLS + RETIRES * len(FAIL_ON)


def test_sliding_window_rate_limiter_fails(inmemory_broker):
    """Normal workflow without limiting would exhaust the API and gets banned."""

    workflow = Workflow(
        broker=inmemory_broker,
    )
    timestamps = []
    workflow.context.state["request_timestamps"] = timestamps

    fetcher = APIFetcherHandler()

    workflow.add_stage(
        "fetcher",
        handler=fetcher,
        context_config=StageContextConfig(
            # Never defer. Use lambda to trigger acquire logic for registering the timestamp ASAP (on predicate broken)
            # using `on_release` would work, but it runs on the task being done (releasing what's been claimed — logical slot).
            predicate=lambda *args: False,
            on_acquire=request_done,
            event="rate-limiter",
            poll_interval=0.05,
        ),
        retry_policy=RetryPolicy(retry_on={RuntimeError}, max_retries=RETIRES),
        task_queue=init_queue("thread", max_workers=4),
    )
    workflow.set_entry_point("fetcher")
    workflow.build()

    # Overwhelm the pipeline with 10 URLs
    for i in range(N_URLS):
        workflow.submit(
            URLInput(url=f"http://api.com/data/{i}", id=str(i)),
        )

    # Wait for all stages to settle
    inmemory_broker.join(timeout=5)
    # With this number of workers, and how lightweight the handler is, we *should* exceed the ratelimit
    assert detect_rate_exceeded(timestamps)
    assert len(timestamps) == N_URLS + RETIRES * len(FAIL_ON)
