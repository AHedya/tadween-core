import time

from tadween_core.task_queue import init_queue


# CPU-bound task. Not using `sum` to cancel any C optimization
def calculate_sum_squares(n_elements=25_000_000):
    data = list(range(n_elements))
    result = 0
    for i in data:
        result += i * i
    return result


thread_queue = init_queue("thread", max_workers=4)

begin = time.perf_counter()
for _ in range(4):
    thread_queue.submit(fn=calculate_sum_squares)
thread_queue.wait_all()

end = time.perf_counter()
# It takes so long in thread queue. It offers no true parallelism due to GIL.
print(f"Thread queue finished after: {end - begin:.3f}")

# Same setup, but using process based executor.
process_queue = init_queue("process", max_workers=4)

begin = time.perf_counter()
for _ in range(4):
    process_queue.submit(fn=calculate_sum_squares)
process_queue.wait_all()

end = time.perf_counter()
print(f"process queue finished after: {end - begin:.3f}")
