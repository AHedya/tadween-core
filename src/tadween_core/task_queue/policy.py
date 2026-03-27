from .base_queue import BaseTaskPolicy


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
