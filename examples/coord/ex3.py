import time

from pydantic import BaseModel

from tadween_core.broker import InMemoryBroker
from tadween_core.handler import BaseHandler
from tadween_core.task_queue import init_queue
from tadween_core.workflow import Workflow

# 1. Define our resources
# We have a global pool of resources.
# For example: 1 GPU (cuda) and 1000MB of RAM.
resources = {"cuda": 1.0, "RAM_MB": 1000.0}


# 2. Define a Resource-Heavy Handler
class HeavyInput(BaseModel):
    id: int


class HeavyHandler(BaseHandler[HeavyInput, HeavyInput]):
    def run(self, inputs: HeavyInput) -> HeavyInput:
        print(f"  [Worker {inputs.id}] Started. Consuming 1 CUDA...")
        time.sleep(1)  # Simulate heavy computation
        print(f"  [Worker {inputs.id}] Finished. Releasing resources.")
        return inputs


def main():

    broker = InMemoryBroker()

    # Initialize Workflow with global resources
    # This creates a ResourceManager internally.
    wf = Workflow(
        broker=broker, resources=resources, default_payload_extractor=lambda x: None
    )

    # 3. Register stages with 'demands'
    # Even if the task queue has 10 threads, the ResourceManager will block
    # the collector thread if the demands cannot be met.
    wf.add_stage(
        "gpu_stage",
        HeavyHandler(),
        demands={"cuda": 1.0, "RAM_MB": 500.0},  # Demands 1 full GPU
        task_queue=init_queue("thread", max_workers=4),
    )
    wf.set_entry_point("gpu_stage")
    wf.build()

    # 4. Submit 3 messages.
    # Since we only have 1 CUDA *resource*, they should process SEQUENTIALLY
    # even though they are independent. Try bumping workflow resources so it can in parallel (or concurrently)
    print("[Main] Submitting 3 heavy tasks...")
    for i in range(1, 5):
        wf.submit({"id": i})

    # Wait for completion
    wf._stages["gpu_stage"].wait_all(timeout=10)
    broker.join(timeout=10)
    wf.close()

    print(
        "[Main] All tasks finished. Notice they ran one by one because of CUDA limits."
    )


if __name__ == "__main__":
    main()
