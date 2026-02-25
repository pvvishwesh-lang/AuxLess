import logging

def get_logger(session_id: str, user_id: str) -> logging.LoggerAdapter:
    logger = logging.getLogger(f"pipeline.{session_id}.{user_id}")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "[%(asctime)s] [session:%(session_id)s] [user:%(user_id)s] %(levelname)s: %(message)s"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logging.LoggerAdapter(logger, {"session_id": session_id, "user_id": user_id})
