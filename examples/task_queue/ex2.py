import random
import time

from tadween_core.task_queue import BaseTaskPolicy, init_queue


class LoggingPolicy(BaseTaskPolicy):
    # policy needs to implement the contract even if it passes some methods.
    # Either inherit from `NoOpPolicy` to skip passing not-needed methods

    def on_done(task_id, future):
        print(f"[{task_id[:8]}] complete")

    def on_running(task_id):
        print(f"[{task_id[:8]}] started processing now")
        pass

    def on_submit(task_id):
        print(f"[{task_id[:8]}] has just been submitted")


def random_sleep(i):
    seconds = round(i * random.random() / 5, 2)
    time.sleep(seconds)
    return i


# single worker threaded task queue
q = init_queue("thread", default_policy=LoggingPolicy, max_workers=1)


begin = time.perf_counter()
for i in range(1, 4):
    if i == 1:
        # override default policy for a given task. First one for example
        q.submit(
            fn=random_sleep,
            i=i,
            # It's recommended to always pass defined function, not lambda expressions.
            # For cleaner code, and not bearing the burden of remembering whether this event needs to picklable or not.
            on_running=lambda tid: print(
                f"[{tid[:8]}] First task ever is being running"
            ),
        )
    else:
        q.submit(fn=random_sleep, i=i)

# lets block until our task finishes.
q.wait_all()

# Note, this statement printed after the task queue finishes all the tasks. q.wait_all().
end = time.perf_counter()
print(f"time taken: {end - begin:.3f}")
