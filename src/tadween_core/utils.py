import logging


def set_logger(level=logging.DEBUG, force: bool = True):
    import logging
    import sys

    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="%(asctime)s:[%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=force,
    )
