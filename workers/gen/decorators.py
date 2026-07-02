import logging
import time
from functools import wraps


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            logging.info(f"Execution time of {func.__name__}: {time.time() - start_time}sec")
    return wrapper
