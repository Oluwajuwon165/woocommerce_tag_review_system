import time
from typing import Callable, TypeVar

T = TypeVar("T")


def retry(func: Callable[[], T], attempts: int = 3, delay: float = 1.0) -> T:
    last_error = None
    for _ in range(attempts):
        try:
            return func()
        except Exception as exc:
            last_error = exc
            time.sleep(delay)
    raise last_error
