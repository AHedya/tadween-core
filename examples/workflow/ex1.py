import time
from dataclasses import dataclass
from typing import Any  # noqa

from tadween_core.broker import (
    InMemoryBroker,
    Message,  # noqa
)
from tadween_core.cache.cache import Cache
from tadween_core.handler.dummy import (  # noqa
    MockDownloadHandler,
    MockDownloadInput,
    MockDownloadOutput,
    NumpySumHandler,
    PythonSumSquaresHandler,
    SumSquaresInput,
    SumSquaresOutput,
)
from tadween_core.stage import DefaultStagePolicy, Stage, StagePolicyBuilder  # noqa
from tadween_core.task_queue import init_queue  # noqa
from tadween_core.workflow import Workflow  # noqa


# Define your cache schema
@dataclass
class CacheSchema:
    s2_input: SumSquaresInput | None = None
    s3_input: MockDownloadInput | None = None


# Three different ways (or approach) to initialize stage policy.
## builder API
S1Policy = (
    StagePolicyBuilder[MockDownloadInput, MockDownloadOutput, CacheSchema]()
    .with_on_success(
        lambda tid, msg, res, broker, repo, cache: cache.set_bucket(
            msg.metadata["artifact_id"],
            CacheSchema(s2_input=SumSquaresInput(n_elements=50_000_000)),
        )
    )
    .with_on_error(lambda msg, err, broker: print(err))
    .with_on_done(
        lambda message, envelope: print(
            f"S1-{message.metadata['artifact_id']}: {envelope.metadata.timings_str}"
        )
    )
)


## Class definition
class S2Policy(DefaultStagePolicy[SumSquaresInput, SumSquaresOutput, CacheSchema]):
    def resolve_inputs(self, message, repo=None, cache=None):
        return cache.get_bucket(message.metadata["artifact_id"]).s2_input

    def on_success(self, task_id, message, result, broker=None, repo=None, cache=None):
        cache.set_entry(
            key=message.metadata["artifact_id"],
            entry_name="s3_input",
            value=MockDownloadInput(file_size_mb=1),
        )

    def on_running(self, task_id, message):
        return super().on_running(task_id, message)

    def on_done(self, message, envelope):
        print(f"S2-{message.metadata['artifact_id']}: {envelope.metadata.timings_str}")


def main():
    begin = time.perf_counter()

    broker = InMemoryBroker()
    cache = Cache(CacheSchema)
    wf = Workflow(broker, cache=cache)

    s1 = Stage(
        name="download",
        handler=MockDownloadHandler(),
        task_queue=init_queue("thread", max_workers=5),
        policy=S1Policy,
    )

    # You can revise our `benchmark/README.md` for more info about _bottleneck_ and some takeaways.
    s2 = Stage(
        name="CPU-Bound",
        handler=PythonSumSquaresHandler(),
        task_queue=init_queue("process", max_workers=4),
        policy=S2Policy(),
    )
    s3 = Stage(
        name="upload",
        handler=MockDownloadHandler(),
        task_queue=init_queue("thread", max_workers=2),
        # Inline builder
        policy=StagePolicyBuilder[MockDownloadInput, SumSquaresOutput, CacheSchema]()
        .with_resolve_inputs(
            lambda msg, repo, cache: (
                cache.get_bucket(msg.metadata["artifact_id"]).s3_input
            )
        )
        .with_on_done(
            lambda message, envelope: print(
                f"S3-{message.metadata['artifact_id']}: {envelope.metadata.timings_str}"
            )
        ),
    )

    wf.integrate_stage("download", s1)
    wf.integrate_stage("sum_square", s2)
    wf.integrate_stage("upload", s3)

    wf.link("download", "sum_square")
    wf.link("sum_square", "upload")

    wf.set_entry_point("download")
    # wf.visualize()
    try:
        wf.build()
        for i in range(10):
            wf.submit(
                MockDownloadInput(file_size_mb=1).model_dump(),
                metadata={"artifact_id": str(i)},
            )
    finally:
        # Wait indefinitely if you're sure nothing would go wrong.
        # Else, put approximated timeout with force=True
        wf.close()

    print(f"Done at {time.perf_counter() - begin:.6f}.")


if __name__ == "__main__":
    main()
