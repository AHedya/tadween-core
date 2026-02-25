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

import logging
import logging.handlers
import multiprocessing
import time
from dataclasses import dataclass
from typing import Any  # noqa

from tadween_core.broker import (
    InMemoryBroker,
    Message,  # noqa
    StatsCollector,
)
from tadween_core.cache.cache import Cache
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

RUN_NAME = "st_try"
ANALYTICS_FILE_PATH = f"/home/projects/tadween/core/experiments/runs/{RUN_NAME}.log"


analytics_queue = multiprocessing.Queue()
analytics_logger = logging.getLogger("analytics")
analytics_logger.setLevel(logging.INFO)
analytics_logger.propagate = False


if not analytics_logger.handlers:
    print("being initialized")
    qh = logging.handlers.QueueHandler(analytics_queue)
    analytics_logger.addHandler(qh)

    fh = logging.FileHandler(ANALYTICS_FILE_PATH, "w", "utf-8")
    fh.setFormatter(logging.Formatter("%(message)s"))
    analytics_listener = logging.handlers.QueueListener(analytics_queue, fh)
    analytics_listener.start()


@dataclass
class CacheSchema:
    s2_input: SumSquaresInput | None = None
    s3_input: MockDownloadInput | None = None


class S1Policy(DefaultStagePolicy[MockDownloadInput, MockDownloadOutput, CacheSchema]):
    def on_success(self, task_id, message, result, broker=None, repo=None, cache=None):
        cache.set_bucket(
            message.metadata["artifact_id"],
            CacheSchema(s2_input=SumSquaresInput(n_elements=10_000_000)),
        )


def main():
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
            lambda msg, env: analytics_logger.info(
                f"S1-{msg.metadata['artifact_id']}: {env.metadata.timings_str}"
            )
        ),
    )

    s2 = Stage(
        name="CPU-Bound",
        handler=PythonSumHandler(),
        task_queue=init_queue("process", max_workers=3),
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
            lambda msg, env: analytics_logger.info(
                f"S2-{msg.metadata['artifact_id']}: {env.metadata.timings_str}"
            )
        ),
    )
    s3 = Stage(
        name="upload",
        handler=MockDownloadHandler(),
        task_queue=init_queue("thread", max_workers=2),
        policy=StagePolicyBuilder[MockDownloadInput, SumSquaresOutput, CacheSchema]()
        .with_on_done(
            lambda msg, env: analytics_logger.info(
                f"S3-{msg.metadata['artifact_id']}: {env.metadata.timings_str}"
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
        analytics_listener.stop()

    print(f"Done at {time.perf_counter() - begin:.6f}.")


if __name__ == "__main__":
    main()
