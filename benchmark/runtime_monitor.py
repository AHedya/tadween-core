"""
make:
    1. multithreaded stage
    2. bottleneck stage: single worker and heavy handler
        2.1 try mp
        2.2 try threading
        2.3 try GIL releasing: numpy, or pytorch
    3. another multithreaded stage
test the workflow efficiency and the bottleneck effect
"""

import multiprocessing as mp  # noqa
import time
from dataclasses import dataclass
from typing import Any, Literal  # noqa

from tadween_core.broker import (
    InMemoryBroker,
    Message,  # noqa
    StatsCollector,
)
from tadween_core.cache.cache import Cache
from tadween_core.devtools.analytics.collectors.runtime import RuntimeCollector
from tadween_core.devtools.analytics.schemas import (  # noqa
    RuntimeMetadata,
    RuntimeSample,
)
from tadween_core.handler.dummy import (  # noqa
    MockDownloadHandler,
    MockDownloadInput,
    MockDownloadOutput,
    NumpySumHandler,
    PythonSumHandler,
    SumSquaresInput,
    SumSquaresOutput,
)
from tadween_core.stage import DefaultStagePolicy, Stage, StagePolicyBuilder  # noqa
from tadween_core.task_queue import init_queue  # noqa
from tadween_core.workflow import Workflow  # noqa

workers: Literal[1, 2, 5] = 1
worker_type: Literal["thread", "process"] = "thread"
start_method: Literal["fork", "spawn"] = "fork"


RUN_NAME = "bottleneck_CPU_numpy"
ANALYTICS_FILE_PATH = f"/home/projects/tadween/core/benchmark/results/{RUN_NAME}.log"


@dataclass
class CacheSchema:
    s2_input: SumSquaresInput | None = None
    s3_input: MockDownloadInput | None = None


def main():
    mp.set_start_method(start_method)
    begin = time.perf_counter()

    broker = InMemoryBroker()
    stat_listener = StatsCollector()
    broker.add_listener(stat_listener)

    cache = Cache(CacheSchema)

    wf = Workflow(broker, cache=cache)

    s1 = Stage(
        name="download",
        handler=MockDownloadHandler(),
        task_queue=init_queue("thread", max_workers=5),
        policy=StagePolicyBuilder[MockDownloadInput, MockDownloadOutput, CacheSchema]()
        .with_on_success(
            lambda tid, msg, res, broker, repo, cache: cache.set_bucket(
                msg.metadata["artifact_id"],
                CacheSchema(s2_input=SumSquaresInput(n_elements=50_000_000)),
            )
        )
        .with_on_done(
            lambda msg, env: runtime_collector.log(
                RuntimeSample(
                    task_id=msg.metadata.get("artifact_id"),
                    stage=msg.metadata.get("current_stage"),
                    waiting=env.metadata.waiting,
                    duration=env.metadata.duration,
                )
            )
        ),
    )

    s2 = Stage(
        name="CPU-Bound",
        handler=NumpySumHandler(),
        task_queue=init_queue(worker_type, max_workers=workers),
        policy=StagePolicyBuilder[SumSquaresInput, SumSquaresOutput, CacheSchema]()
        .with_resolve_inputs(
            lambda msg, repo, cache: cache.get_bucket(
                msg.metadata["artifact_id"]
            ).s2_input
        )
        .with_on_success(
            lambda tid, msg, res, broker, repo, cache: cache.set_entry(
                key=msg.metadata["artifact_id"],
                entry_name="s3_input",
                value=MockDownloadInput(file_size_mb=1),
            )
        )
        .with_on_done(
            lambda msg, env: runtime_collector.log(
                RuntimeSample(
                    task_id=msg.metadata.get("artifact_id"),
                    stage=msg.metadata.get("current_stage"),
                    waiting=env.metadata.waiting,
                    duration=env.metadata.duration,
                )
            )
        ),
    )
    s3 = Stage(
        name="upload",
        handler=MockDownloadHandler(),
        task_queue=init_queue("thread", max_workers=5),
        policy=StagePolicyBuilder[MockDownloadInput, SumSquaresOutput, CacheSchema]()
        .with_on_done(
            lambda msg, env: runtime_collector.log(
                RuntimeSample(
                    task_id=msg.metadata.get("artifact_id"),
                    stage=msg.metadata.get("current_stage"),
                    waiting=env.metadata.waiting,
                    duration=env.metadata.duration,
                )
            )
        )
        .with_resolve_inputs(
            lambda msg, repo, cache: cache.get_bucket(
                msg.metadata["artifact_id"]
            ).s3_input
        ),
    )

    wf.integrate_stage("download", s1)
    wf.integrate_stage("sum_square", s2)
    wf.integrate_stage("upload", s3)

    wf.link("download", "sum_square")
    wf.link("sum_square", "upload")

    wf.set_entry_point("download")

    try:
        wf.build()
        for i in range(20):
            wf.submit(
                MockDownloadInput(file_size_mb=1).model_dump(),
                metadata={"artifact_id": str(i)},
            )
    finally:
        wf.close(3)
        runtime_collector.stop()

    print(f"Done at {time.perf_counter() - begin:.6f}.")


if __name__ == "__main__":
    worker_tag = (
        f"{worker_type[0]}{workers}-{start_method}"
        if worker_type == "process"
        else f"{worker_type[0]}{workers}"
    )
    title = f"{worker_tag}"
    runtime_collector = RuntimeCollector(
        ANALYTICS_FILE_PATH,
        file_mode="append",
        start_session=True,
        title=title,
        # description="5 workers for threaded download, and same with upload but bottleneck-ed middle CPU bound stage (our variable)",
    )

    main()
