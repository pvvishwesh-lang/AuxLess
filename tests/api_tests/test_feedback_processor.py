import json
import pytest
from unittest.mock import patch, MagicMock, call


class TestParseFeedbackFn:
    def test_valid_record(self):
        from backend.pipelines.feedback_processor import ParseFeedbackFn
        fn = ParseFeedbackFn()
        record = {
            "session_id": "sess1",
            "user_id": "user1",
            "video_id": "vid1",
            "action": "like"
        }
        result = list(fn.process(json.dumps(record).encode("utf-8")))
        assert len(result) == 1
        assert result[0]["session_id"] == "sess1"

    def test_missing_fields_yields_nothing(self):
        from backend.pipelines.feedback_processor import ParseFeedbackFn
        fn = ParseFeedbackFn()
        record = {"session_id": "sess1", "user_id": "user1"}
        result = list(fn.process(json.dumps(record).encode("utf-8")))
        assert len(result) == 0

    def test_invalid_json_yields_nothing(self):
        from backend.pipelines.feedback_processor import ParseFeedbackFn
        fn = ParseFeedbackFn()
        result = list(fn.process(b"not json"))
        assert len(result) == 0


class TestFilterBySessionFn:
    def test_matching_session_yields(self):
        from backend.pipelines.feedback_processor import FilterBySessionFn
        fn = FilterBySessionFn("sess1")
        result = list(fn.process({"session_id": "sess1", "action": "like"}))
        assert len(result) == 1

    def test_non_matching_session_filtered(self):
        from backend.pipelines.feedback_processor import FilterBySessionFn
        fn = FilterBySessionFn("sess1")
        result = list(fn.process({"session_id": "sess2", "action": "like"}))
        assert len(result) == 0


class TestScoreFeedbackFn:
    def test_like_scores_positive(self):
        from backend.pipelines.feedback_processor import ScoreFeedbackFn
        fn = ScoreFeedbackFn()
        result = list(fn.process({"action": "like", "video_id": "vid1"}))
        assert result[0]["score_delta"] == 2.0

    def test_dislike_scores_negative(self):
        from backend.pipelines.feedback_processor import ScoreFeedbackFn
        fn = ScoreFeedbackFn()
        result = list(fn.process({"action": "dislike", "video_id": "vid1"}))
        assert result[0]["score_delta"] == -2.0

    def test_skip_scores_small_negative(self):
        from backend.pipelines.feedback_processor import ScoreFeedbackFn
        fn = ScoreFeedbackFn()
        result = list(fn.process({"action": "skip", "video_id": "vid1"}))
        assert result[0]["score_delta"] == -0.5

    def test_replay_scores_positive(self):
        from backend.pipelines.feedback_processor import ScoreFeedbackFn
        fn = ScoreFeedbackFn()
        result = list(fn.process({"action": "replay", "video_id": "vid1"}))
        assert result[0]["score_delta"] == 1.5

    def test_unknown_action_scores_zero(self):
        from backend.pipelines.feedback_processor import ScoreFeedbackFn
        fn = ScoreFeedbackFn()
        result = list(fn.process({"action": "unknown", "video_id": "vid1"}))
        assert result[0]["score_delta"] == 0.0


class TestUpdateFirestoreFn:
    @patch("backend.pipelines.feedback_processor.firestore.Client")
    def test_writes_to_tracks_and_user_feedback(self, mock_fs_cls):
        from backend.pipelines.feedback_processor import UpdateFirestoreFn

        mock_db = MagicMock()
        mock_fs_cls.return_value = mock_db

        fn = UpdateFirestoreFn(
            project_id="test-proj",
            database_id="test-db",
            session_id="sess1"
        )
        fn.setup()

        element = {
            "video_id": "vid1",
            "user_id": "user1",
            "action": "like",
            "score_delta": 2.0,
        }

        result = list(fn.process(element))
        assert len(result) == 1

        track_set_call = (
            mock_db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        assert track_set_call.call_count == 2  

    @patch("backend.pipelines.feedback_processor.firestore.Client")
    def test_user_feedback_doc_id_format(self, mock_fs_cls):
        from backend.pipelines.feedback_processor import UpdateFirestoreFn

        mock_db = MagicMock()
        mock_fs_cls.return_value = mock_db

        fn = UpdateFirestoreFn(
            project_id="test-proj",
            database_id="test-db",
            session_id="sess1"
        )
        fn.setup()

        element = {
            "video_id": "vid1",
            "user_id": "user1",
            "action": "dislike",
            "score_delta": -2.0,
        }

        list(fn.process(element))

        collection_calls = mock_db.collection.return_value.document.return_value.collection.call_args_list
        document_calls = (
            mock_db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.call_args_list
        )

        collection_names = [c[0][0] for c in collection_calls]
        assert "user_feedback" in collection_names

        doc_ids = [c[0][0] for c in document_calls]
        assert "user1_vid1" in doc_ids
