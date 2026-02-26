def set_logger(force: bool = True):
    import logging
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stdout,
        format="%(asctime)s:[%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=force,
    )
