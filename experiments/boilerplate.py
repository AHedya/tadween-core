import time

from pydantic import BaseModel  # noqa

from tadween_core.broker import Message
from tadween_core.handler import BaseHandler  # noqa


class AddInput(BaseModel):
    value: int
    operand: int = 1


class AddOutput(BaseModel):
    result: int


class AddHandler(BaseHandler[AddInput, AddOutput]):
    def run(self, inputs: AddInput) -> AddOutput:
        res = inputs.value + inputs.operand
        return AddOutput(result=res)


class MultiplyInput(BaseModel):
    value: int
    factor: int = 2


class MultiplyOutput(BaseModel):
    final_value: int


class MultiplyHandler(BaseHandler[MultiplyInput, MultiplyOutput]):
    def run(self, inputs):
        res = inputs.value * inputs.factor
        return MultiplyOutput(final_value=res)


def heavy_result_fn(size_mb):
    size = max(size_mb * 1048576, 1048576)
    dumb_data = bytearray(size)
    time.sleep(0.2)
    return dumb_data


def heavy_comp_fn(limit: int = 10_000_000):
    x = 0
    for i in range(limit):
        x = (i * 2) / 3 + 10
    return x


def wait_fn(seconds: float = 1):
    time.sleep(seconds)


def make_handler(broker, publish_message):
    def handler(msg: Message):
        broker.publish(Message(**publish_message))

    return handler
