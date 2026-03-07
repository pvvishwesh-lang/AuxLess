import pytest
from unittest.mock import MagicMock, patch
from backend.pipelines.api.retry_utils import retry


class TestRetry:

    def test_succeeds_on_first_try(self):
        mock_fn = MagicMock(return_value="ok")

        @retry(retries=3, delay=0)
        def fn():
            return mock_fn()

        assert fn() == "ok"
        assert mock_fn.call_count == 1

    def test_retries_on_failure_then_succeeds(self):
        mock_fn = MagicMock(side_effect=[Exception("fail"), Exception("fail"), "ok"])

        @retry(retries=3, delay=0)
        def fn():
            return mock_fn()

        with patch("time.sleep"):
            result = fn()

        assert result == "ok"
        assert mock_fn.call_count == 3

    def test_raises_after_all_retries_exhausted(self):
        mock_fn = MagicMock(side_effect=Exception("always fails"))

        @retry(retries=3, delay=0)
        def fn():
            return mock_fn()

        with patch("time.sleep"):
            with pytest.raises(Exception, match="always fails"):
                fn()

        assert mock_fn.call_count == 3

    def test_backoff_capped_at_30_seconds(self):
        mock_fn = MagicMock(side_effect=Exception("fail"))

        @retry(retries=4, delay=20)
        def fn():
            return mock_fn()

        sleep_calls = []
        with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            with pytest.raises(Exception):
                fn()

        assert all(s <= 30 for s in sleep_calls), f"Backoff exceeded 30s: {sleep_calls}"

    def test_preserves_function_name(self):
        @retry(retries=2, delay=0)
        def my_function():
            pass

        assert my_function.__name__ == "my_function"
