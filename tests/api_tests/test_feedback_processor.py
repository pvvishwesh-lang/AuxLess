import json
import pytest
from unittest.mock import MagicMock
from backend.pipelines.streaming.feedback_processor import (
    ParseFeedbackFn,
    FilterBySessionFn,
    ScoreFeedbackFn,
)


def _encode(event: dict) -> bytes:
    return json.dumps(event).encode("utf-8")


def _run_dofn(fn, items):
    results = []
    for item in items:
        results.extend(list(fn.process(item)))
    return results


class TestParseFeedbackFn:

    def test_valid_like_event_parsed(self):
        event = {"room_id": "r1", "song_id": "s1", "event_type": "like", "user_id": "u1"}
        results = _run_dofn(ParseFeedbackFn(), [_encode(event)])
        assert len(results) == 1
        assert results[0]["action"] == "like"

    def test_invalid_json_produces_no_output(self):
        assert _run_dofn(ParseFeedbackFn(), [b"not json at all"]) == []

    def test_missing_room_id_produces_no_output(self):
        event = {"song_id": "s1", "event_type": "like", "user_id": "u1"}
        assert _run_dofn(ParseFeedbackFn(), [_encode(event)]) == []

    def test_unknown_action_produces_no_output(self):
        event = {"room_id": "r1", "song_id": "s1", "event_type": "explode", "user_id": "u1"}
        assert _run_dofn(ParseFeedbackFn(), [_encode(event)]) == []

    def test_all_valid_actions_accepted(self):
        for action in ("play", "skip", "complete", "like", "dislike", "replay"):
            event = {"room_id": "r1", "song_id": "s1", "event_type": action, "user_id": "u1"}
            results = _run_dofn(ParseFeedbackFn(), [_encode(event)])
            assert len(results) == 1, f"Action '{action}' was rejected"


class TestFilterBySessionFn:

    def test_matching_session_passes(self):
        event = {"room_id": "room_001", "song_id": "s1", "action": "like"}
        assert len(_run_dofn(FilterBySessionFn("room_001"), [event])) == 1

    def test_non_matching_session_filtered(self):
        event = {"room_id": "room_002", "song_id": "s1", "action": "like"}
        assert _run_dofn(FilterBySessionFn("room_001"), [event]) == []


class TestScoreFeedbackFn:

    def _score(self, action):
        event = {"room_id": "r1", "song_id": "s1", "action": action}
        return _run_dofn(ScoreFeedbackFn(), [event])[0]["score_delta"]

    def test_like_adds_positive_score(self):
        assert self._score("like") > 0

    def test_dislike_adds_negative_score(self):
        assert self._score("dislike") < 0

    def test_skip_adds_negative_score(self):
        assert self._score("skip") < 0

    def test_replay_adds_positive_score(self):
        assert self._score("replay") > 0

    def test_unknown_action_scores_zero(self):
        assert self._score("unknown_xyz") == 0.0
