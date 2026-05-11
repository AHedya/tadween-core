from dataclasses import dataclass

RETRY_COUNT_ENTRY = "__retries_count"
RETRY_MAX_ENTRY = "__max_retries"


@dataclass(slots=True)
class RetryPolicy:
    retry_on: set[type[Exception] | str]
    max_retries: int = 3
