"""
Tests for recommendation/firestore_client.py

Covers:
  - get_db()
  - get_session_track_scores()
  - get_session_play_sequence()
  - get_session_user_ids()
  - get_songs_played_count()
  - update_last_refreshed_at()
  - is_session_active()
"""

import os
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_mock_db():
    return MagicMock()


def make_track_doc(video_id, score=1.0, like_count=1, skip_count=0,
                   dislike_count=0, replay_count=0):
    doc = MagicMock()
    doc.id = video_id
    doc.to_dict.return_value = {
        "score":         score,
        "like_count":    like_count,
        "skip_count":    skip_count,
        "dislike_count": dislike_count,
        "replay_count":  replay_count,
        "last_updated":  None,
    }
    return doc


def make_event_doc(video_id, play_order, liked_flag):
    doc = MagicMock()
    doc.to_dict.return_value = {
        "video_id":   video_id,
        "play_order": play_order,
        "liked_flag": liked_flag,
    }
    return doc


def make_session_doc(users=None, exists=True, status="active"):
    doc = MagicMock()
    doc.exists = exists
    doc.to_dict.return_value = {
        "users":  users or [],
        "status": status,
    }
    return doc


def wire_collection(db, collection_name, document_name,
                    subcollection_name=None, stream_docs=None,
                    get_doc=None):
    """
    Wires db.collection(...).document(...)[.collection(...).stream()] or .get()
    """
    coll  = db.collection.return_value
    ddoc  = coll.document.return_value
    if subcollection_name:
        subcoll = ddoc.collection.return_value
        if stream_docs is not None:
            subcoll.stream.return_value = stream_docs
            subcoll.order_by.return_value.stream.return_value = stream_docs
    if get_doc is not None:
        ddoc.get.return_value = get_doc
    return db


# ── get_db tests ──────────────────────────────────────────────────────────────

class TestGetDb:

    @patch("ml.recommendation.firestore_client.firestore.Client")
    def test_returns_client(self, mock_fs):
        from ml.recommendation.firestore_client import get_db
        mock_fs.return_value = MagicMock()
        result = get_db("proj", "db")
        assert result is not None

    @patch("ml.recommendation.firestore_client.firestore.Client")
    def test_uses_project_id(self, mock_fs):
        from ml.recommendation.firestore_client import get_db
        get_db("my-project", "my-db")
        mock_fs.assert_called_once_with(project="my-project", database="my-db")

    @patch("ml.recommendation.firestore_client.firestore.Client")
    def test_uses_database_id(self, mock_fs):
        from ml.recommendation.firestore_client import get_db
        get_db("proj", "auxless")
        call_kwargs = mock_fs.call_args[1]
        assert call_kwargs["database"] == "auxless"


# ── get_session_track_scores tests ────────────────────────────────────────────

class TestGetSessionTrackScores:

    def _call(self, db, session_id="sess_1"):
        from ml.recommendation.firestore_client import get_session_track_scores
        return get_session_track_scores(db, session_id)

    def test_returns_dict(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0")])
        result = self._call(db)
        assert isinstance(result, dict)

    def test_correct_video_id_key(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0")])
        result = self._call(db)
        assert "vid_0" in result

    def test_score_field_present(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0", score=2.0)])
        result = self._call(db)
        assert "score" in result["vid_0"]

    def test_correct_score_value(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0", score=2.0)])
        result = self._call(db)
        assert result["vid_0"]["score"] == 2.0

    def test_like_count_field(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0", like_count=3)])
        result = self._call(db)
        assert result["vid_0"]["like_count"] == 3

    def test_skip_count_field(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0", skip_count=1)])
        result = self._call(db)
        assert result["vid_0"]["skip_count"] == 1

    def test_dislike_count_field(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0", dislike_count=2)])
        result = self._call(db)
        assert result["vid_0"]["dislike_count"] == 2

    def test_replay_count_field(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0", replay_count=4)])
        result = self._call(db)
        assert result["vid_0"]["replay_count"] == 4

    def test_empty_tracks_returns_empty_dict(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks", stream_docs=[])
        result = self._call(db)
        assert result == {}

    def test_multiple_tracks(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[
                            make_track_doc("vid_0", score=1.0),
                            make_track_doc("vid_1", score=-1.0),
                        ])
        result = self._call(db)
        assert "vid_0" in result
        assert "vid_1" in result

    def test_exception_returns_empty_dict(self):
        db = make_mock_db()
        db.collection.side_effect = Exception("Firestore error")
        result = self._call(db)
        assert result == {}

    def test_last_updated_field(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "tracks",
                        stream_docs=[make_track_doc("vid_0")])
        result = self._call(db)
        assert "last_updated" in result["vid_0"]

    def test_default_score_is_zero(self):
        db  = make_mock_db()
        doc = MagicMock()
        doc.id = "vid_empty"
        doc.to_dict.return_value = {}
        wire_collection(db, "sessions", "sess_1", "tracks", stream_docs=[doc])
        result = self._call(db)
        assert result["vid_empty"]["score"] == 0.0


# ── get_session_play_sequence tests ──────────────────────────────────────────

class TestGetSessionPlaySequence:

    def _call(self, db, session_id="sess_1"):
        from ml.recommendation.firestore_client import get_session_play_sequence
        return get_session_play_sequence(db, session_id)

    def test_returns_list(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "song_events",
                        stream_docs=[make_event_doc("vid_0", 1, 1)])
        result = self._call(db)
        assert isinstance(result, list)

    def test_correct_video_id(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "song_events",
                        stream_docs=[make_event_doc("vid_0", 1, 1)])
        result = self._call(db)
        assert result[0]["video_id"] == "vid_0"

    def test_correct_play_order(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "song_events",
                        stream_docs=[make_event_doc("vid_0", 3, 1)])
        result = self._call(db)
        assert result[0]["play_order"] == 3

    def test_correct_liked_flag(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "song_events",
                        stream_docs=[make_event_doc("vid_0", 1, 0)])
        result = self._call(db)
        assert result[0]["liked_flag"] == 0

    def test_empty_sequence_returns_empty_list(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "song_events", stream_docs=[])
        result = self._call(db)
        assert result == []

    def test_multiple_events(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "song_events",
                        stream_docs=[
                            make_event_doc("vid_0", 1, 1),
                            make_event_doc("vid_1", 2, 0),
                            make_event_doc("vid_2", 3, 1),
                        ])
        result = self._call(db)
        assert len(result) == 3

    def test_exception_returns_empty_list(self):
        db = make_mock_db()
        db.collection.side_effect = Exception("Firestore error")
        result = self._call(db)
        assert result == []

    def test_each_event_has_required_keys(self):
        db = make_mock_db()
        wire_collection(db, "sessions", "sess_1", "song_events",
                        stream_docs=[make_event_doc("vid_0", 1, 1)])
        result = self._call(db)
        assert "video_id"   in result[0]
        assert "play_order" in result[0]
        assert "liked_flag" in result[0]

    def test_missing_fields_default_to_zero(self):
        db  = make_mock_db()
        doc = MagicMock()
        doc.to_dict.return_value = {}
        wire_collection(db, "sessions", "sess_1", "song_events",
                        stream_docs=[doc])
        result = self._call(db)
        assert result[0]["play_order"] == 0
        assert result[0]["liked_flag"] == 0


# ── get_session_user_ids tests ────────────────────────────────────────────────

class TestGetSessionUserIds:

    def _call(self, db, session_id="sess_1"):
        from ml.recommendation.firestore_client import get_session_user_ids
        return get_session_user_ids(db, session_id)

    def test_returns_list(self):
        db  = make_mock_db()
        doc = make_session_doc(users=[
            {"user_id": "u1", "isactive": True},
        ])
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert isinstance(result, list)

    def test_returns_active_user_ids(self):
        db  = make_mock_db()
        doc = make_session_doc(users=[
            {"user_id": "u1", "isactive": True},
            {"user_id": "u2", "isactive": True},
        ])
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert "u1" in result
        assert "u2" in result

    def test_excludes_inactive_users(self):
        db  = make_mock_db()
        doc = make_session_doc(users=[
            {"user_id": "u1", "isactive": True},
            {"user_id": "u2", "isactive": False},
        ])
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert "u2" not in result

    def test_session_not_found_returns_empty(self):
        db  = make_mock_db()
        doc = make_session_doc(exists=False)
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert result == []

    def test_empty_users_returns_empty(self):
        db  = make_mock_db()
        doc = make_session_doc(users=[])
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert result == []

    def test_exception_returns_empty(self):
        db = make_mock_db()
        db.collection.side_effect = Exception("error")
        result = self._call(db)
        assert result == []

    def test_skips_users_without_user_id(self):
        db  = make_mock_db()
        doc = make_session_doc(users=[
            {"user_id": "u1", "isactive": True},
            {"isactive": True},   # no user_id
        ])
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert len(result) == 1

    def test_returns_correct_count(self):
        db  = make_mock_db()
        doc = make_session_doc(users=[
            {"user_id": f"u{i}", "isactive": True} for i in range(5)
        ])
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert len(result) == 5

    def test_default_isactive_true(self):
        db  = make_mock_db()
        doc = make_session_doc(users=[
            {"user_id": "u1"},   # no isactive field → defaults to True
        ])
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert "u1" in result


# ── get_songs_played_count tests ──────────────────────────────────────────────

class TestGetSongsPlayedCount:

    def _call(self, db, session_id="sess_1"):
        from ml.recommendation.firestore_client import get_songs_played_count
        return get_songs_played_count(db, session_id)

    def test_returns_int(self):
        db  = make_mock_db()
        doc = MagicMock()
        doc.exists = True
        doc.to_dict.return_value = {"songs_played_count": 5}
        db.collection.return_value.document.return_value \
          .collection.return_value.document.return_value.get.return_value = doc
        result = self._call(db)
        assert isinstance(result, int)

    def test_returns_correct_count(self):
        db  = make_mock_db()
        doc = MagicMock()
        doc.exists = True
        doc.to_dict.return_value = {"songs_played_count": 7}
        db.collection.return_value.document.return_value \
          .collection.return_value.document.return_value.get.return_value = doc
        result = self._call(db)
        assert result == 7

    def test_returns_zero_if_doc_not_exists(self):
        db  = make_mock_db()
        doc = MagicMock()
        doc.exists = False
        db.collection.return_value.document.return_value \
          .collection.return_value.document.return_value.get.return_value = doc
        result = self._call(db)
        assert result == 0

    def test_returns_zero_on_exception(self):
        db = make_mock_db()
        db.collection.side_effect = Exception("Firestore error")
        result = self._call(db)
        assert result == 0

    def test_returns_zero_if_key_missing(self):
        db  = make_mock_db()
        doc = MagicMock()
        doc.exists = True
        doc.to_dict.return_value = {}
        db.collection.return_value.document.return_value \
          .collection.return_value.document.return_value.get.return_value = doc
        result = self._call(db)
        assert result == 0


# ── update_last_refreshed_at tests ────────────────────────────────────────────

class TestUpdateLastRefreshedAt:

    def test_calls_set(self):
        from ml.recommendation.firestore_client import update_last_refreshed_at
        db  = make_mock_db()
        set_mock = db.collection.return_value.document.return_value \
                     .collection.return_value.document.return_value.set
        update_last_refreshed_at(db, "sess_1", 10)
        set_mock.assert_called_once()

    def test_passes_correct_count(self):
        from ml.recommendation.firestore_client import update_last_refreshed_at
        db  = make_mock_db()
        set_mock = db.collection.return_value.document.return_value \
                     .collection.return_value.document.return_value.set
        update_last_refreshed_at(db, "sess_1", 15)
        call_args = set_mock.call_args[0][0]
        assert call_args["last_refreshed_at"] == 15

    def test_uses_merge_true(self):
        from ml.recommendation.firestore_client import update_last_refreshed_at
        db  = make_mock_db()
        set_mock = db.collection.return_value.document.return_value \
                     .collection.return_value.document.return_value.set
        update_last_refreshed_at(db, "sess_1", 10)
        call_kwargs = set_mock.call_args[1]
        assert call_kwargs.get("merge") is True

    def test_exception_does_not_raise(self):
        from ml.recommendation.firestore_client import update_last_refreshed_at
        db = make_mock_db()
        db.collection.side_effect = Exception("error")
        # should not raise
        update_last_refreshed_at(db, "sess_1", 10)


# ── is_session_active tests ───────────────────────────────────────────────────

class TestIsSessionActive:

    def _call(self, db, session_id="sess_1"):
        from ml.recommendation.firestore_client import is_session_active
        return is_session_active(db, session_id)

    def test_returns_true_for_active(self):
        db  = make_mock_db()
        doc = make_session_doc(status="active")
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        assert self._call(db) is True

    def test_returns_true_for_done(self):
        db  = make_mock_db()
        doc = make_session_doc(status="done")
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        assert self._call(db) is True

    def test_returns_false_for_ended(self):
        db  = make_mock_db()
        doc = make_session_doc(status="ended")
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        assert self._call(db) is False

    def test_returns_false_if_not_exists(self):
        db  = make_mock_db()
        doc = make_session_doc(exists=False)
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        assert self._call(db) is False

    def test_returns_false_on_exception(self):
        db = make_mock_db()
        db.collection.side_effect = Exception("error")
        assert self._call(db) is False

    def test_returns_bool(self):
        db  = make_mock_db()
        doc = make_session_doc(status="active")
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        result = self._call(db)
        assert isinstance(result, bool)

    def test_returns_false_for_unknown_status(self):
        db  = make_mock_db()
        doc = make_session_doc(status="unknown_status")
        wire_collection(db, "sessions", "sess_1", get_doc=doc)
        assert self._call(db) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
