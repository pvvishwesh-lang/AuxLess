import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 30

def retry(retries: int = 3, delay: float = 2):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    wait = min(delay * (2 ** attempt), MAX_BACKOFF_SECONDS)
                    logger.warning(f"[{fn.__name__}] Attempt {attempt + 1}/{retries} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
            logger.error(f"[{fn.__name__}] All {retries} retries exhausted. Last error: {last_exception}")
            raise last_exception
        return wrapper
    return decorator
