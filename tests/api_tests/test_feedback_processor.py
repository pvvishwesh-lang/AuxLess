import json
import pytest
from backend.pipelines.feedback_processor import (
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
        event = {"session_id": "sess_001", "user_id": "u1", "video_id": "abc123", "action": "like"}
        results = _run_dofn(ParseFeedbackFn(), [_encode(event)])
        assert len(results) == 1
        assert results[0]["action"] == "like"

    def test_invalid_json_produces_no_output(self):
        assert _run_dofn(ParseFeedbackFn(), [b"not json at all"]) == []

    def test_missing_required_field_produces_no_output(self):
        event = {"session_id": "sess_001", "user_id": "u1", "action": "like"}
        assert _run_dofn(ParseFeedbackFn(), [_encode(event)]) == []

    def test_unknown_action_still_passes_parse(self):
        event = {"session_id": "sess_001", "user_id": "u1", "video_id": "abc123", "action": "explode"}
        results = _run_dofn(ParseFeedbackFn(), [_encode(event)])
        assert len(results) == 1

    def test_all_required_fields_accepted(self):
        for action in ("like", "dislike", "skip", "replay"):
            event = {"session_id": "s1", "user_id": "u1", "video_id": "v1", "action": action}
            results = _run_dofn(ParseFeedbackFn(), [_encode(event)])
            assert len(results) == 1, f"Action '{action}' was rejected"


class TestFilterBySessionFn:

    def test_matching_session_passes(self):
        event = {"session_id": "sess_001", "video_id": "v1", "action": "like"}
        assert len(_run_dofn(FilterBySessionFn("sess_001"), [event])) == 1

    def test_non_matching_session_filtered(self):
        event = {"session_id": "sess_002", "video_id": "v1", "action": "like"}
        assert _run_dofn(FilterBySessionFn("sess_001"), [event]) == []


class TestScoreFeedbackFn:

    def _score(self, action):
        event = {"session_id": "s1", "video_id": "v1", "action": action}
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
