import pytest


@pytest.fixture
def valid_raw_record():
    return {
        "playlist_name":   "Chill Vibes",
        "track_title":     "Blinding Lights",
        "artist_name":     "The Weeknd",
        "video_id":        "abc123",
        "genre":           "Pop",
        "country":         "USA",
        "collection_name": "After Hours",
        "collection_id":   "123456",
        "trackTimeMillis": "200000",
        "view_count":      "1000000",
        "like_count":      "50000",
        "comment_count":   "3000",
    }


@pytest.fixture
def derived_record(valid_raw_record):
    from backend.pipelines.api.validation_utils import derive_fields
    return derive_fields(dict(valid_raw_record))


@pytest.fixture
def valid_feedback_event():
    return {
        "room_id":    "room_001",
        "song_id":    "abc123",
        "song_name":  "Blinding Lights",
        "artist":     "The Weeknd",
        "event_type": "like",
        "user_id":    "user_xyz",
        "timestamp":  "2026-02-25T10:00:00Z",
    }


@pytest.fixture
def sample_csv_content():
    return (
        "playlist_name,track_title,artist_name,video_id,genre,country,"
        "collection_name,collection_id,trackTimeMillis,trackTimeSeconds,"
        "view_count,like_count,comment_count,like_to_view_ratio,comment_to_view_ratio\n"
        "Chill,Blinding Lights,The Weeknd,abc123,Pop,USA,After Hours,111,200000,200,"
        "1000000,50000,3000,0.05,0.003\n"
        "Party,Shape of You,Ed Sheeran,def456,Pop,GBR,Divide,222,210000,210,"
        "2000000,80000,5000,0.04,0.0025\n"
        "Chill,Unknown Song,Unknown,ghi789,Unknown,Unknown,,,0,0,0,0,0,0.0,0.0\n"
    )import json
import pytest


@pytest.fixture
def valid_raw_record():
    return {
        "playlist_name":   "Chill Vibes",
        "track_title":     "Blinding Lights",
        "artist_name":     "The Weeknd",
        "video_id":        "abc123",
        "genre":           "Pop",
        "country":         "USA",
        "collection_name": "After Hours",
        "collection_id":   "123456",
        "trackTimeMillis": "200000",
        "view_count":      "1000000",
        "like_count":      "50000",
        "comment_count":   "3000",
    }


@pytest.fixture
def derived_record(valid_raw_record):
    from backend.pipelines.api.validation_utils import derive_fields
    return derive_fields(dict(valid_raw_record))


@pytest.fixture
def valid_feedback_event():
    return {
        "room_id":    "room_001",
        "song_id":    "abc123",
        "song_name":  "Blinding Lights",
        "artist":     "The Weeknd",
        "event_type": "like",
        "user_id":    "user_xyz",
        "timestamp":  "2026-02-25T10:00:00Z",
    }


@pytest.fixture
def sample_csv_content():
    return (
        "playlist_name,track_title,artist_name,video_id,genre,country,"
        "collection_name,collection_id,trackTimeMillis,trackTimeSeconds,"
        "view_count,like_count,comment_count,like_to_view_ratio,comment_to_view_ratio\n"
        "Chill,Blinding Lights,The Weeknd,abc123,Pop,USA,After Hours,111,200000,200,"
        "1000000,50000,3000,0.05,0.003\n"
        "Party,Shape of You,Ed Sheeran,def456,Pop,GBR,Divide,222,210000,210,"
        "2000000,80000,5000,0.04,0.0025\n"
        "Chill,Unknown Song,Unknown,ghi789,Unknown,Unknown,,,0,0,0,0,0,0.0,0.0\n"
    )
