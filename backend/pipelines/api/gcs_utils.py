import time
from google.cloud import storage

def combine_gcs_files_safe(bucket_name, input_prefix, output_file, retries=3, delay=2):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for attempt in range(retries):
        try:
            blobs = list(bucket.list_blobs(prefix=input_prefix))
            combined_lines = []
            header = None
            for blob in blobs:
                if blob.name.endswith('.csv') and 'Final_Output' not in blob.name:
                    content = blob.download_as_text().splitlines()
                    if not content: continue
                    if header is None: header = content[0]
                    body = [l for l in content if l != header]
                    combined_lines.extend(body)
            if header and combined_lines:
                out = header + "\n" + "\n".join(combined_lines)
                bucket.blob(output_file).upload_from_string(out)
            print(f"Combined {len(blobs)} files into {output_file}")
            break
        except Exception as e:
            print(f"Attempt {attempt+1} failed for combining GCS files: {e}")
            time.sleep(delay * (2 ** attempt))
            if attempt == retries - 1:
                raise e