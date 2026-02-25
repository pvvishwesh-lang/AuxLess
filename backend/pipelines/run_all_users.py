import os
import json
import logging
from google.cloud import storage
from backend.pipelines.api.firestore_client import FirestoreClient
from backend.pipelines.Api_Puller import run_pipeline_for_user
from backend.pipelines.api.gcs_utils import combine_gcs_files_safe
from backend.pipelines.api.bias_analyser import compute_bias_metrics
from backend.pipelines.api.schema_validator import run_schema_validation
from backend.pipelines.api.alert_manager import run_anomaly_checks_and_alert
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_intermediate_files(bucket_name: str, prefix: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        blob.delete()
    logger.info(f"Cleaned up intermediate files under gs://{bucket_name}/{prefix}")

def write_json_to_gcs(bucket_name: str, blob_path: str, data: dict) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(data=json.dumps(data, indent=2),content_type="application/json")
    full_path = f"gs://{bucket_name}/{blob_path}"
    logger.info(f"Written to {full_path}")
    return full_path

def run_for_session(session_id: str):
    bucket = os.environ["BUCKET"]
    project_id = os.environ["PROJECT_ID"]
    database_id = os.environ["FIRESTORE_DATABASE"]
    fs = FirestoreClient(project_id, database_id)
    status = fs.get_session_status(session_id)
    if status not in ["running"]:
        logger.info(f"Session {session_id} is '{status}', not 'running'. Skipping.")
        return
    users = fs.get_session_users(session_id)
    if not users:
        raise RuntimeError(f"No active users found in session {session_id}")
    logger.info(f"Starting pipelines for {len(users)} users in session {session_id}")
    prefix_valid = f"user_outputs/{session_id}/valid"
    prefix_invalid = f"user_outputs/{session_id}/invalid"
    jobs = []
    for user_id, refresh_token in users:
        try:
            logger.info(f"Submitting Dataflow job for user: {user_id}")
            job = run_pipeline_for_user(user_id=user_id,refresh_token=refresh_token,bucket=bucket,prefix_valid=prefix_valid,prefix_invalid=prefix_invalid,session_id=session_id)
            if job:
                jobs.append(job)
        except Exception as e:
            logger.error(f"Failed to submit pipeline for user {user_id}: {e}")
    if not jobs:
        logger.error("No Dataflow jobs submitted. Marking session as error.")
        fs.update_session_status(session_id, "error")
        return
    logger.info(f"Waiting for {len(jobs)} Dataflow jobs to complete...")
    for job in jobs:
        try:
            job.wait_until_finish()
        except Exception as e:
            logger.error(f"Error waiting for Dataflow job: {e}")
    final_csv_path = None
    combined_valid_path = f"Final_Output/{session_id}_valid/{session_id}_combined_valid.csv"
    combined_invalid_path = f"Final_Output/{session_id}_invalid/{session_id}_combined_invalid.csv"
    try:
        logger.info("Combining valid output files...")
        combine_gcs_files_safe(bucket_name=bucket,input_prefix=prefix_valid,output_file=combined_valid_path)
        logger.info("Combining invalid output files...")
        combine_gcs_files_safe(bucket_name=bucket,input_prefix=prefix_invalid,output_file=combined_invalid_path)
        client = storage.Client()
        blob = client.bucket(bucket).blob(combined_valid_path)
        if not blob.exists():
            raise RuntimeError(f"Combined valid file not found: gs://{bucket}/{combined_valid_path}")
        final_csv_path = f"gs://{bucket}/{combined_valid_path}"
        logger.info(f"Combined valid file verified at {final_csv_path}")
        cleanup_intermediate_files(bucket, prefix_valid)
        cleanup_intermediate_files(bucket, prefix_invalid)
        fs.update_session_status(session_id, "done")
        logger.info(f"Session {session_id} completed successfully.")
    except Exception as e:
        logger.error(f"Combination/cleanup failed: {e}")
        fs.update_session_status(session_id, "error")
    if not final_csv_path:
        logger.warning("Skipping post-processing steps: final CSV unavailable.")
        return
    try:
        logger.info("Computing bias metrics...")
        bias_summary = compute_bias_metrics(final_csv_path, slice_cols=["genre", "country"])
        write_json_to_gcs(bucket, f"Final_Output/{session_id}_bias_metrics/{session_id}_bias_metrics.json", bias_summary)
        logger.info("Bias metrics saved.")
    except Exception as e:
        logger.error(f"Bias metrics failed: {e}")
    try:
        logger.info("Running schema validation...")
        schema_report = run_schema_validation(bucket, session_id)
        write_json_to_gcs(bucket, f"Final_Output/{session_id}_schema_report/{session_id}_schema_report.json", schema_report)
        logger.info(f"Schema valid: {schema_report['schema_valid']}, violations: {len(schema_report['schema_violations'])}")
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
    try:
        logger.info("Running session anomaly checks...")
        run_anomaly_checks_and_alert(bucket, session_id)
    except Exception as e:
        logger.error(f"Anomaly alert failed: {e}")
