import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime


class TestSyncSessionToBigquery:
    @patch("backend.pipelines.api.bq_sync.bigquery.Client")
    @patch("backend.pipelines.api.bq_sync.firestore.Client")
    def test_no_feedback_skips_sync(self, mock_fs_cls, mock_bq_cls):
        mock_db = MagicMock()
        mock_fs_cls.return_value = mock_db
        mock_db.collection.return_value.document.return_value.collection.return_value.stream.return_value = []

        from backend.pipelines.api.bq_sync import sync_session_to_bigquery
        sync_session_to_bigquery(
            session_id="sess_empty",
            project_id="test-proj",
            database_id="test-db",
            user_ids=["user1"]
        )

        mock_bq_cls.return_value.insert_rows_json.assert_not_called()

    @patch("backend.pipelines.api.bq_sync.bigquery.Client")
    @patch("backend.pipelines.api.bq_sync.firestore.Client")
    def test_feedback_creates_history_rows(self, mock_fs_cls, mock_bq_cls):
        mock_db = MagicMock()
        mock_fs_cls.return_value = mock_db

        mock_doc = MagicMock()
        mock_doc.to_dict.return_value = {
            "user_id": "vishwesh",
            "video_id": "vid123",
            "action": "like",
            "score_delta": 2.0,
            "last_updated": datetime(2026, 3, 22, 12, 0, 0),
        }
        mock_db.collection.return_value.document.return_value.collection.return_value.stream.return_value = [mock_doc]

        mock_bq = MagicMock()
        mock_bq_cls.return_value = mock_bq
        mock_bq.insert_rows_json.return_value = []
        mock_bq.query.return_value.result.return_value = []

        from backend.pipelines.api.bq_sync import sync_session_to_bigquery
        sync_session_to_bigquery(
            session_id="sess_test",
            project_id="test-proj",
            database_id="test-db",
            user_ids=["vishwesh"]
        )

        mock_bq.insert_rows_json.assert_called_once()
        rows = mock_bq.insert_rows_json.call_args[0][1]
        assert len(rows) == 1
        assert rows[0]["user_id"] == "vishwesh"
        assert rows[0]["action"] == "like"
        assert rows[0]["video_id"] == "vid123"

    @patch("backend.pipelines.api.bq_sync.bigquery.Client")
    @patch("backend.pipelines.api.bq_sync.firestore.Client")
    def test_per_user_preferences_separated(self, mock_fs_cls, mock_bq_cls):
        mock_db = MagicMock()
        mock_fs_cls.return_value = mock_db

        doc1 = MagicMock()
        doc1.to_dict.return_value = {
            "user_id": "user_a",
            "video_id": "song1",
            "action": "like",
            "score_delta": 2.0,
            "last_updated": datetime(2026, 3, 22, 12, 0, 0),
        }
        doc2 = MagicMock()
        doc2.to_dict.return_value = {
            "user_id": "user_b",
            "video_id": "song1",
            "action": "dislike",
            "score_delta": -2.0,
            "last_updated": datetime(2026, 3, 22, 12, 0, 1),
        }
        mock_db.collection.return_value.document.return_value.collection.return_value.stream.return_value = [doc1, doc2]

        mock_bq = MagicMock()
        mock_bq_cls.return_value = mock_bq
        mock_bq.insert_rows_json.return_value = []
        mock_bq.query.return_value.result.return_value = []

        from backend.pipelines.api.bq_sync import sync_session_to_bigquery
        sync_session_to_bigquery(
            session_id="sess_multi",
            project_id="test-proj",
            database_id="test-db",
            user_ids=["user_a", "user_b"]
        )

        rows = mock_bq.insert_rows_json.call_args[0][1]
        assert len(rows) == 2

        assert mock_bq.query.call_count >= 2

    @patch("backend.pipelines.api.bq_sync.bigquery.Client")
    @patch("backend.pipelines.api.bq_sync.firestore.Client")
    def test_skips_incomplete_feedback(self, mock_fs_cls, mock_bq_cls):
        mock_db = MagicMock()
        mock_fs_cls.return_value = mock_db

        bad_doc = MagicMock()
        bad_doc.to_dict.return_value = {
            "user_id": "vishwesh",
            "video_id": None,
            "action": "like",
        }
        mock_db.collection.return_value.document.return_value.collection.return_value.stream.return_value = [bad_doc]

        mock_bq = MagicMock()
        mock_bq_cls.return_value = mock_bq
        mock_bq.insert_rows_json.return_value = []

        from backend.pipelines.api.bq_sync import sync_session_to_bigquery
        sync_session_to_bigquery(
            session_id="sess_bad",
            project_id="test-proj",
            database_id="test-db"
        )

        if mock_bq.insert_rows_json.called:
            rows = mock_bq.insert_rows_json.call_args[0][1]
            assert len(rows) == 0


class TestMergeUserPreferences:
    @patch("backend.pipelines.api.bq_sync.bigquery.Client")
    def test_insert_new_user(self, mock_bq_cls):
        mock_bq = MagicMock()
        mock_bq.query.return_value.result.return_value = []

        from backend.pipelines.api.bq_sync import _merge_user_preferences
        _merge_user_preferences(
            bq=mock_bq,
            table_id="proj.dataset.users",
            user_id="new_user",
            prefs={"liked": {"song1"}, "disliked": set(), "skipped": set(), "replayed": set()}
        )

        assert mock_bq.query.call_count == 2

    @patch("backend.pipelines.api.bq_sync.bigquery.Client")
    def test_update_existing_user(self, mock_bq_cls):
        mock_bq = MagicMock()

        mock_check_result = [MagicMock(user_id="existing_user")]

        mock_fetch_row = MagicMock()
        mock_fetch_row.liked_songs = ["old_song"]
        mock_fetch_row.disliked_songs = []
        mock_fetch_row.skipped_songs = []
        mock_fetch_row.replayed_songs = []
        mock_fetch_result = [mock_fetch_row]

        mock_bq.query.return_value.result.side_effect = [
            mock_check_result,
            mock_fetch_result,
            None, 
        ]

        from backend.pipelines.api.bq_sync import _merge_user_preferences
        _merge_user_preferences(
            bq=mock_bq,
            table_id="proj.dataset.users",
            user_id="existing_user",
            prefs={"liked": {"new_song"}, "disliked": set(), "skipped": set(), "replayed": set()}
        )

        assert mock_bq.query.call_count == 3
