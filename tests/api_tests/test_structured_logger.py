import logging
import pytest
from backend.pipelines.api.structured_logger import get_logger


class TestGetLogger:

    def test_returns_logger_adapter(self):
        assert isinstance(get_logger("sess_001", "user_001"), logging.LoggerAdapter)

    def test_extra_contains_session_and_user(self):
        logger = get_logger("sess_002", "user_002")
        assert logger.extra["session_id"] == "sess_002"
        assert logger.extra["user_id"]    == "user_002"

    def test_same_logger_reused(self):
        logger1 = get_logger("sess_003", "user_003")
        logger2 = get_logger("sess_003", "user_003")
        assert logger1.logger is logger2.logger

    def test_different_sessions_different_loggers(self):
        logger1 = get_logger("sess_A", "user_A")
        logger2 = get_logger("sess_B", "user_B")
        assert logger1.logger is not logger2.logger
