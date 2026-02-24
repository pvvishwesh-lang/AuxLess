import logging

def get_logger(session_id, user_id):
    logger = logging.getLogger(f"{session_id}_{user_id}")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '[%(asctime)s] [session:%(session_id)s] [user:%(user_id)s] %(levelname)s: %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    extra = {'session_id': session_id, 'user_id': user_id}
    return logging.LoggerAdapter(logger, extra)