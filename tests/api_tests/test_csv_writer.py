import pytest
from backend.pipelines.api.csv_writer import dict_to_csv_line

COLUMNS = ["track_title", "artist_name", "video_id", "genre"]


class TestDictToCsvLine:

    def test_basic_output(self):
        record = {"track_title": "Song", "artist_name": "Artist", "video_id": "v1", "genre": "Pop"}
        assert dict_to_csv_line(record, COLUMNS) == "Song,Artist,v1,Pop"

    def test_missing_key_outputs_empty_string(self):
        record = {"track_title": "Song", "video_id": "v1", "genre": "Pop"}
        assert dict_to_csv_line(record, COLUMNS) == "Song,,v1,Pop"

    def test_value_with_comma_is_quoted(self):
        record = {"track_title": "Hello, World", "artist_name": "X", "video_id": "v1", "genre": "Pop"}
        assert '"Hello, World"' in dict_to_csv_line(record, COLUMNS)

    def test_empty_record(self):
        assert dict_to_csv_line({}, COLUMNS) == ",,,"

    def test_all_empty_strings(self):
        record = {col: "" for col in COLUMNS}
        assert dict_to_csv_line(record, COLUMNS) == ",,,"
