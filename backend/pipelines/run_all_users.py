# run_all_users.py
import os
from backend.pipelines.api.firestore_client import FirestoreClient
from backend.pipelines.Api_Puller import run_pipeline_for_user
from google.cloud import storage
import time

def combine_gcs_files(bucket_name, input_prefix, output_file):
    time.sleep(5)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=input_prefix))
    print(f"Found {len(blobs)} files to combine in {input_prefix}")

    combined_lines = []
    header=None
    for blob in blobs:
        if blob.name.endswith('.csv') and 'Final_Output' not in blob.name:
            content=blob.download_as_text().splitlines()
            if not content: 
                continue
            if header is None:
                header = content[0]
            body = [l for l in content if l != header]
            combined_lines.extend(body)

    if header and combined_lines:
        out = header+"\n"+'\n'.join(combined_lines)
        bucket.blob(output_file).upload_from_string(out)
        print(f"Combined {len(blobs)} files into {output_file}")

def run_for_session(session_id):
    project_id = os.environ["PROJECT_ID"]
    database_id = os.environ["FIRESTORE_DATABASE"]

    fs = FirestoreClient(project_id, database_id)

    users = fs.get_session_users(session_id)
    if not users:
        raise RuntimeError("No active users in session")

    fs.update_session_status(session_id, "running")
    threads=[]
    for user_id,refresh_token in users:
        t=threading.Thread(
            target=run_pipeline_for_user,
            args=(user_id, refresh_token, prefix,session_id)
        )
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    time.sleep(10)
    bucket = "youtube-pipeline-staging-bucket"
    prefix = f"user_outputs/{session_id}/"
    combine_gcs_files(
        bucket,
        prefix,
        f"Final_Output/{session_id}_combined.csv"
    )

    fs.update_session_status(session_id, "done")
