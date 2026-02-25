import time

from pydantic import BaseModel

from tadween_core.broker import InMemoryBroker
from tadween_core.handler import BaseHandler
from tadween_core.workflow import Workflow


# 1. Define models
class RawInput(BaseModel):
    text: str


class UppercaseOutput(BaseModel):
    text: str


class FinalOutput(BaseModel):
    text: str
    length: int


# 2. Implement handlers
class UppercaseHandler(BaseHandler[RawInput, UppercaseOutput]):
    def run(self, inputs: RawInput) -> UppercaseOutput:
        print(f"[Stage: Uppercase] Processing: {inputs.text}")
        return UppercaseOutput(text=inputs.text.upper())


class LengthHandler(BaseHandler[UppercaseOutput, FinalOutput]):
    def run(self, inputs: UppercaseOutput) -> FinalOutput:
        print(f"[Stage: Length] Processing: {inputs.text}")
        return FinalOutput(text=inputs.text, length=len(inputs.text))


def main():
    # 3. Initialize core components
    broker = InMemoryBroker()
    workflow = Workflow(broker=broker, name="SimplePipeline")

    # 4. Add stages
    workflow.add_stage("uppercase", UppercaseHandler())
    workflow.add_stage("length", LengthHandler())

    # 5. Link stages: uppercase -> length
    workflow.link("uppercase", "length")
    workflow.set_entry_point("uppercase")

    # 6. Build the workflow
    print("[System] Building workflow...")
    workflow.build()

    # 7. Submit a message to the workflow
    print("[Client] Submitting data to workflow...")
    msg_id = workflow.submit(RawInput(text="hello tadween"))
    print(f"[Client] Submitted message ID: {msg_id}")

    # 8. Wait for workflow to process
    # Note: In a real app, you'd subscribe to the final topic or check a repo
    time.sleep(1.0)

    # Shutdown
    print("[System] Closing workflow...")
    workflow.close(timeout=5.0)


if __name__ == "__main__":
    main()
