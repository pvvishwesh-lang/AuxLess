import pytest
from backend.pipelines.api.csv_writer import dict_to_csv_line

COLUMNS = ["track_title", "artist_name", "video_id", "genre"]


class TestDictToCsvLine:

    def test_basic_output(self):
        record = {"track_title": "Song", "artist_name": "Artist", "video_id": "v1", "genre": "Pop"}
        result = dict_to_csv_line(record, COLUMNS)
        assert result == "Song,Artist,v1,Pop"

    def test_missing_key_outputs_empty_string(self):
        record = {"track_title": "Song", "video_id": "v1", "genre": "Pop"}
        result = dict_to_csv_line(record, COLUMNS)
        assert result == "Song,,v1,Pop"

    def test_value_with_comma_is_quoted(self):
        record = {"track_title": "Hello, World", "artist_name": "X", "video_id": "v1", "genre": "Pop"}
        result = dict_to_csv_line(record, COLUMNS)
        assert '"Hello, World"' in result

    def test_value_with_newline_is_quoted(self):
        record = {"track_title": "Line1\nLine2", "artist_name": "X", "video_id": "v1", "genre": "Pop"}
        result = dict_to_csv_line(record, COLUMNS)
        assert "\n" not in result.split(",")[0] or '"' in result

    def test_empty_record(self):
        result = dict_to_csv_line({}, COLUMNS)
        assert result == ",,,"

    def test_all_empty_strings(self):
        record = {col: "" for col in COLUMNS}
        result = dict_to_csv_line(record, COLUMNS)
        assert result == ",,,"
