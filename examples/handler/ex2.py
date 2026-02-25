from pydantic import BaseModel

from tadween_core.handler import HandlerFactory
from tadween_core.handler.base import BaseHandler


class PrettyInput(BaseModel):
    message: str
    prefix: str
    suffix: str = "."


class PrettyOutput(BaseModel):
    output: str


# normal handler definition
class PrettyHandler(BaseHandler[PrettyInput, PrettyOutput]):
    def run(self, inputs):
        output = f"{inputs.prefix}{inputs.message}{inputs.suffix}"
        print(output)
        return PrettyOutput(output=output)


class PrettyFactory(HandlerFactory):
    def create():
        return PrettyHandler()


def main():
    x = PrettyFactory.create()
    x(PrettyInput(message="Hello", prefix="", suffix="!"))


if __name__ == "__main__":
    main()
