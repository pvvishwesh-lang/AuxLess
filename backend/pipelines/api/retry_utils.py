import time
import logging
from functools import wraps

def retry(retries=3, delay=2):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logging.warning(f"Retry {i+1}/{retries} failed: {e}")
                    time.sleep(delay * (2 ** i))
            logging.error(f"All retries failed: {last_exception}")
            raise last_exception
        return wrapper
    return decorator