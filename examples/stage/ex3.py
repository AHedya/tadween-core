from dataclasses import dataclass

from tadween_core.cache import Cache  # noqa
from tadween_core.handler.dummy import (  # noqa
    PythonSumSquaresHandler,
    SumSquaresInput,
    SumSquaresOutput,
)
from tadween_core.stage import (  # noqa
    DefaultStagePolicy,
    Stage,
    StagePolicy,
)
from tadween_core.stage.policy import InterceptionContext
from tadween_core.task_queue import init_queue


# define cache bucket schema
@dataclass
class CacheSchema:
    value: SumSquaresOutput


cache = Cache(CacheSchema)


# Note: we've now added `CacheSchema`. The third generic in our policy.
class SumSquarePolicy(
    DefaultStagePolicy[SumSquaresInput, SumSquaresOutput, CacheSchema]
):
    def on_done(self, message, envelope):
        multiplier = message.metadata.get("i", 0)
        print(f"Multiplier [{multiplier}] task took: {envelope.metadata.duration:.5}s")

    def intercept(self, message, broker=None, repo=None, cache=None):
        multiplier = str(message.metadata.get("i", 0))
        cached = cache.get_bucket(multiplier)
        if cached:
            self.on_success("SKIPPED", message, cached.value, broker, repo, cache)
            return InterceptionContext(intercepted=True, reason="cached")
        else:
            return InterceptionContext(intercepted=False)

    def on_success(self, task_id, message, result, broker=None, repo=None, cache=None):
        multiplier = str(message.metadata.get("i", 0))
        cached = cache.get_bucket(multiplier)

        if cached is None:
            # set cache
            cache.set_bucket(str(multiplier), CacheSchema(value=result))
        else:
            print("Cache-Hit! ", end="")
        print(f"Passed! [{multiplier}]")


def main():
    # simulate what an upstream stage would have written to cache.
    pre_computed = [3, 7, 9]
    for i in pre_computed:
        # set dummy value
        result = SumSquaresOutput(result=1e10)
        cache.set_bucket(str(i), CacheSchema(value=result))

    stage = Stage(
        PythonSumSquaresHandler(),
        policy=SumSquarePolicy(),
        task_queue=init_queue("process", max_workers=5),
        cache=cache,
    )

    for i in range(1, 11):
        stage.submit({"n_elements": 1_000_000 * i}, metadata={"i": i})
    # wait until current work finishes
    stage.task_queue.wait_all()

    # Mimic another dependent stage. All tasks are cache-hit
    for i in range(1, 11):
        stage.submit({"n_elements": 1_000_000 * i}, metadata={"i": i})
    stage.close()

    # Important note: `intercept` runs synchronously on the main thread at submission time.
    # `on_success` runs asynchronously after the worker completes.
    # When you submit all tasks in rapid succession, every `intercept` call hits an empty cache
    # because no task has had time to finish and write to it.


if __name__ == "__main__":
    main()
