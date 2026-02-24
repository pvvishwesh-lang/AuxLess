import os
from backend.pipelines.api.firestore_client import FirestoreClient
from backend.pipelines.Api_Puller import run_pipeline_for_user
from google.cloud import storage
import time
from backend.pipelines.api.gcs_utils import combine_gcs_files_safe
from backend.pipelines.api.bias_analyser import compute_bias_metrics
        
def cleanup_intermediate_files(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        if prefix in blob.name:
            blob.delete()
    print(f"Cleaned up intermediate files in {prefix}")


def write_bias_metrics_to_gcs(bucket_name, session_id, bias_summary):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_path = f'Final_Output/{session_id}_bias_metrics.json'
    blob = bucket.blob(blob_path)
    blob.upload_from_string(data=json.dumps(bias_summary, indent=2),content_type='application/json')
    print(f"Bias metrics written to gs://{bucket_name}/{blob_path}")
    return f"gs://{bucket_name}/{blob_path}"

def run_for_session(session_id):
    bucket = os.environ['BUCKET']
    prefix_valid = f"user_outputs/{session_id}/valid/"
    prefix_invalid = f"user_outputs/{session_id}/invalid/"
    project_id = os.environ["PROJECT_ID"]
    database_id = os.environ["FIRESTORE_DATABASE"]
    fs = FirestoreClient(project_id, database_id)
    users = fs.get_session_users(session_id)
    if not users:
        raise RuntimeError("No active users in session")
    status=fs.get_session_status(session_id)
    if status not in ['running']:
        print(f"Session {session_id} already processed or not in triggered state. Skipping.")
        return
    print(f"Users: {users}")
    print(f"Starting pipelines for {len(users)} users...")
    jobs=[]
    for user_id, refresh_token in users:
        try:
            print(f"Submitting Dataflow job for user: {user_id}")
            job=run_pipeline_for_user(user_id, refresh_token, prefix_valid, session_id)
            if job:
                jobs.append(job)
        except Exception as e:
            print(f"Failed pipeline for user {user_id}: {e}")
    if not jobs:
        print("No jobs submitted successfully. Marking session as error.")
        fs.update_session_status(session_id,'error')
        return
    print(f'Waiting for {len(jobs)} jobs to complete...')
    for job in jobs:
        try:
            job.wait_until_finish()
        except Exception as e:
            print(f'Error waiting for job: {e}')
    try:
        combined_valid_path = f'Final_Output/{session_id}_combined_valid.csv'
        combine_gcs_files_safe(bucket, prefix_valid, combined_valid_path)
        combined_invalid_path = f'Final_Output/{session_id}_combined_invalid.csv'
        combine_gcs_files_safe(bucket, prefix_invalid, combined_invalid_path)
        cleanup_intermediate_files(bucket, prefix_valid)
        cleanup_intermediate_files(bucket, prefix_invalid)
        fs.update_session_status(session_id, "done")
        print(f"Session {session_id} completed successfully.")
        final_csv_path = f'gs://{bucket}/{combined_valid_path}'
        bias_summary = compute_bias_metrics(final_csv_path, slice_cols=['genre', 'country'])
        write_bias_metrics_to_gcs(bucket, session_id, bias_summary)
    except Exception as e:
        print(f"Final combination/cleanup failed: {e}")
        fs.update_session_status(session_id, "error")
        
    
