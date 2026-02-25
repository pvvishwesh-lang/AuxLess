import time
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)


def combine_gcs_files_safe(bucket_name: str,input_prefix: str,output_file: str,retries: int = 3,delay: float = 2):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for attempt in range(retries):
        try:
            blobs = [ b for b in bucket.list_blobs(prefix=input_prefix) if b.name.endswith(".csv") and "Final_Output" not in b.name]
            if not blobs:
                logger.warning(f"No CSV files found under gs://{bucket_name}/{input_prefix}")
                return
            header = None
            body_lines = []
            for i, blob in enumerate(blobs):
                lines = blob.download_as_text().splitlines()
                if not lines:
                    continue
                if header is None:
                    header = lines[0]
                body_lines.extend(lines[1:])
            if header:
                output = header + "\n" + "\n".join(body_lines)
                bucket.blob(output_file).upload_from_string(output)
                logger.info(f"Combined {len(blobs)} files into gs://{bucket_name}/{output_file}")
            return
        except Exception as e:
            wait = delay * (2 ** attempt)
            logger.warning(f"Combine attempt {attempt + 1}/{retries} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            if attempt == retries - 1:
                raise
