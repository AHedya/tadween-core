from .base_queue import BaseTaskPolicy


class LoggingPolicy(BaseTaskPolicy):
    @staticmethod
    def on_done(task_id, future):
        x = future.result()
        if x.success:
            print(f"Task: {task_id}. Done in {x.metadata.duration:0.3f}s")
        else:
            print(f"Failed, error: {x.error}")


class NoOpPolicy(BaseTaskPolicy):
    """Default policy that does nothing."""

    @staticmethod
    def on_submit(task_id) -> None:
        pass

    @staticmethod
    def on_running(task_id) -> None:
        pass

    @staticmethod
    def on_done(task_id, future) -> None:
        pass
