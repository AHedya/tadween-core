import logging

from tadween_core import set_logger
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
from tadween_core.task_queue import init_queue

set_logger(logging.INFO)


# define stage policy.
# Pass handler i/o types get full type hinting
# Use `StagePolicy` if you'd implement all the hooks, or `DefaultStagePolicy` if you'd implement some
class SumSquarePolicy(DefaultStagePolicy[SumSquaresInput, SumSquaresOutput]):
    def on_done(self, message, envelope):
        multiplier = message.metadata.get("i", 0)
        print(
            f"Multiplier [{multiplier}] task took: {envelope.metadata.duration:.5}s. Waited for {envelope.metadata.waiting:.3}"
        )

    def on_success(self, task_id, message, result, broker=None, repo=None, cache=None):
        # Typically, on_success would be used to save results or fire another stage by publishing a message to the broker.
        # However, for simplicity, just print that you've you've succeeded.
        multiplier = message.metadata.get("i", 0)
        print(f"[{multiplier}] result: {result.result}")


def main():
    # use process for CPU-bound tasks. You'll notice the difference if you switch to "thread" instead of "process"
    tq = init_queue("process", max_workers=4)
    # inject task queue dependency
    stage = Stage(
        PythonSumSquaresHandler(),
        policy=SumSquarePolicy(),
        task_queue=tq,
    )

    for i in range(10):
        # You can use typed input instead of dict
        stage.submit(
            SumSquaresInput(n_elements=1_000_000 * (i + 1)),
            metadata={"i": i + 1},
        )
    stage.close()


if __name__ == "__main__":
    main()
