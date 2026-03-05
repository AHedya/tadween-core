from tadween_core import set_logger
from tadween_core.handler.dummy import (
    PythonSumSquaresHandler,
)
from tadween_core.stage import Stage

set_logger()


def main():
    # minimal
    stage = Stage(PythonSumSquaresHandler())

    for _ in range(10):
        # Note how stage input is inferred from handler type.
        stage.submit({"n_elements": 1_000_000})

    stage.close()


if __name__ == "__main__":
    main()
