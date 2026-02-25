import random
import time

from tadween_core.task_queue import init_queue


def random_sleep(i):
    seconds = round(i * random.random() / 10, 2)
    time.sleep(seconds)
    print(f"[{i}] Slept for {seconds}")


# defaults to threads task queue
q = init_queue()


begin = time.perf_counter()
for i in range(1, 10):
    q.submit(fn=random_sleep, i=i)
end = time.perf_counter()

# Few milliseconds. Async execution
print(f"Submitted after: {end - begin:.3f}")
