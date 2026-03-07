import pytest
from unittest.mock import patch, MagicMock
from backend.pipelines.api.gcs_utils import combine_gcs_files_safe


def _make_blob(name, content):
    blob = MagicMock()
    blob.name = name
    blob.download_as_text.return_value = content
    return blob


class TestCombineGcsFilesSafe:

    def test_combines_two_files_correctly(self):
        blob1 = _make_blob("prefix/u1_valid.csv", "col_a,col_b\nval1,val2")
        blob2 = _make_blob("prefix/u2_valid.csv", "col_a,col_b\nval3,val4")

        mock_output_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = [blob1, blob2]
        mock_bucket.blob.return_value = mock_output_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch("backend.pipelines.api.gcs_utils.storage.Client", return_value=mock_client):
            combine_gcs_files_safe("my-bucket", "prefix", "Final_Output/out.csv")

        uploaded = mock_output_blob.upload_from_string.call_args[0][0]
        assert "col_a,col_b" in uploaded
        assert "val1,val2"   in uploaded
        assert "val3,val4"   in uploaded
        assert uploaded.count("col_a,col_b") == 1

    def test_skips_non_csv_files(self):
        blob_csv  = _make_blob("prefix/file.csv",  "h\nrow")
        blob_json = _make_blob("prefix/file.json", "not csv")

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = [blob_csv, blob_json]
        mock_bucket.blob.return_value = MagicMock()
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch("backend.pipelines.api.gcs_utils.storage.Client", return_value=mock_client):
            combine_gcs_files_safe("my-bucket", "prefix", "output.csv")

        blob_json.download_as_text.assert_not_called()

    def test_skips_final_output_files(self):
        blob_skip = _make_blob("Final_Output/already_combined.csv", "h\nrow")
        blob_ok   = _make_blob("prefix/valid.csv", "h\nrow2")

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = [blob_skip, blob_ok]
        mock_bucket.blob.return_value = MagicMock()
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch("backend.pipelines.api.gcs_utils.storage.Client", return_value=mock_client):
            combine_gcs_files_safe("my-bucket", "prefix", "output.csv")

        blob_skip.download_as_text.assert_not_called()

    def test_retries_on_failure(self):
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.side_effect = [Exception("GCS error"), Exception("GCS error"), []]
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch("backend.pipelines.api.gcs_utils.storage.Client", return_value=mock_client):
            with patch("time.sleep"):
                combine_gcs_files_safe("my-bucket", "prefix", "output.csv", retries=3)

        assert mock_bucket.list_blobs.call_count == 3

    def test_raises_after_all_retries_fail(self):
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.side_effect = Exception("persistent error")
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch("backend.pipelines.api.gcs_utils.storage.Client", return_value=mock_client):
            with patch("time.sleep"):
                with pytest.raises(Exception, match="persistent error"):
                    combine_gcs_files_safe("my-bucket", "prefix", "output.csv", retries=3)
