import time

from tadween_core.task_queue import ThreadTaskQueue
from tadween_core.task_queue.base import TaskStatus


def sleep_fn(i):
    time.sleep(1)
    return i


# threaded task queue
q = ThreadTaskQueue(
    name="TTQ-1",
    max_workers=1,
)


id0 = q.submit(fn=sleep_fn, i=0, retain_result=True)


task_status = q.get_status(id0)
while task_status != TaskStatus.COMPLETED:
    print(task_status)
    time.sleep(0.5)
    task_status = q.get_status(id0)
else:
    print(f"completed: {task_status}")
    print("[id0] result: ", q.get_result(task_id=id0, consume=False))

    # Notice we consume on read
    print("[id0] result: ", q.get_result(task_id=id0, consume=True))

    try:
        q.get_result(task_id=id0, consume=True)
    except ValueError as e:
        print("[id0] Consumed.", e)


# simplified replica
id1 = q.submit(fn=sleep_fn, i=1, retain_result=True)
try:
    # Make sure to set consume to False if you're just checking. Or use get_status instead of get_result. `get_status` doesn't consume.
    # However, you need to make sure that retain_result is set to True for this task as consuming removes task id.
    res1 = q.get_result(id1, timeout=0.2, consume=False)
except TimeoutError:
    print("[id1] Still not done")

try:
    # plenty of time + consume on read.
    res1 = q.get_result(id1, timeout=2)
    print(f"[id1] result: {res1}")
    # already consumed. Raise error
    res1 = q.get_result(id1)
except ValueError:
    print("[id1] consumed")


for i in range(5):
    q.submit(sleep_fn, i=i, retain_result=True)

# stream results as done
for res in q.stream_completed():
    print(res)
