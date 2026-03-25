import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.textio import WriteToText
from backend.pipelines.api.auth import GoogleAuthClient
from backend.pipelines.api.beam_extensions import ValidatingDoFn
from backend.pipelines.api.csv_writer import dict_to_csv_line
from backend.pipelines.api.ReadFromAPI import ReadFromAPI
import os
import re
import uuid
import json
COLUMNS = [
    "playlist_name", "track_title", "artist_name", "video_id", "genre",
    "country", "collection_name", "collection_id", "trackTimeMillis",
    "trackTimeSeconds", "view_count", "like_count", "comment_count",
    "like_to_view_ratio", "comment_to_view_ratio"
]

def sanitize_for_job_name(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9-]", "-", s)
    s = re.sub(r"-+", "-", s)
    s = s.strip("-")
    if not s or not s[0].isalpha():
        s = "a" + s
    if not s[-1].isalnum():
        s = s + "1"
    return s

def run_pipeline_for_user(user_id: str,refresh_token: str,bucket: str,prefix_valid: str,prefix_invalid: str,session_id: str):
    auth = GoogleAuthClient(
        token_uri=os.environ["TOKEN_URI"],
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
        redirect_uri=os.environ["REDIRECT_URIS"],
        refresh_token=refresh_token
    )
<<<<<<< HEAD
    access_token = auth.get_access_token()
    unique_id = str(uuid.uuid4())[:4]
    options = PipelineOptions(["--setup_file=./setup.py"])
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = os.environ["PROJECT_ID"]
    gcp_options.job_name = (
        f"youtube-pipeline-{session_id[:10]}-"
        f"{sanitize_for_job_name(user_id)[:10]}-{unique_id}"
    )
    gcp_options.region = "europe-west1"
    gcp_options.staging_location = "gs://youtube-pipeline-staging-bucket/staging"
    gcp_options.temp_location = "gs://youtube-pipeline-staging-bucket/temp"
    gcp_options.service_account_email = os.environ["SERVICE_ACCOUNT_EMAIL"]
    options.view_as(StandardOptions).runner = "DataflowRunner"
    valid_gcs_prefix = f"gs://{bucket}/{prefix_valid}/{user_id}_valid"
    invalid_gcs_prefix = f"gs://{bucket}/{prefix_invalid}/{user_id}_invalid"
    p = beam.Pipeline(options=options)
    validated = (
        p
        | "Seed"            >> beam.Create([None])
        | "Read From API"   >> beam.ParDo(ReadFromAPI(access_token))
        | "Validate"        >> beam.ParDo(ValidatingDoFn(access_token, session_id=session_id, user_id=user_id)).with_outputs("invalid_records", main="valid")
    )

    (
        validated.valid
        | "Valid To CSV"    >> beam.Map(lambda r: dict_to_csv_line(r, COLUMNS))
        | "Write Valid"     >> WriteToText(file_path_prefix=valid_gcs_prefix,file_name_suffix=".csv",shard_name_template="",header=",".join(COLUMNS))
    )
    (
        validated.invalid_records
        | "Invalid To CSV"  >> beam.Map(lambda r: f'{r["error"]},"{json.dumps(r["record"])}"')
        | "Write Invalid"   >> WriteToText(file_path_prefix=invalid_gcs_prefix,file_name_suffix=".csv",shard_name_template="",header="error,record_json"
        )
    )

    return p.run()
=======
    unique_id=str(uuid.uuid4())[:4]
    access_token=auth.get_access_token()
    yt_client=YoutubeClient(access_token)
    columns=['playlist_name', 'track_title', 'artist_name', 'video_id', 'genre','country','collection_name','collection_id','trackTimeMillis', 'view_count','like_count','comment_count']
    job_name = f"youtube-pipeline-{session_id[:10]}-{sanitize_for_job_name(user_id)[:10]}-{unique_id}" # ← NEW

    run_metadata = {                                                   # ← NEW
        "job_name": job_name,                                          # ← NEW
        "gcs_prefix": gcs_prefix,                                      # ← NEW
        "columns": columns,                                            # ← NEW
    }                                                                  # ← NEW
    write_manifest(user_id, session_id, run_metadata)                 # ← NEW
    options = PipelineOptions(["--setup_file=./setup.py"])
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = os.environ['PROJECT_ID']
    google_cloud_options.job_name = f"youtube-pipeline-{session_id[:10]}-{sanitize_for_job_name(user_id)[:10]}-{unique_id}"
    google_cloud_options.region='us-central1'
    google_cloud_options.staging_location = "gs://youtube-pipeline-staging-bucket/staging"
    google_cloud_options.temp_location = "gs://youtube-pipeline-staging-bucket/temp"
    options.view_as(StandardOptions).runner = "DataflowRunner"
    google_cloud_options.service_account_email='serviceaccountforgithub@main-shade-485500-a0.iam.gserviceaccount.com'
    try:                                                               # ← NEW
        p=beam.Pipeline(options=options)
        (
                p
                |'Seed'>>beam.Create([None])
                |'Read From API'>>beam.ParDo(ReadFromAPI(access_token))
                | 'Snapshot'     >> SnapshotAPIRows(user_id, session_id)  # ← NEW
                |'ToCSV' >> beam.Map(lambda r: dict_to_csv_line(r, columns))
                |'WriteToGCS'>> WriteToText(file_path_prefix=f'gs://youtube-pipeline-staging-bucket/{gcs_prefix}{user_id}',file_name_suffix='.csv',shard_name_template='',header=','.join(columns))
        )
        result = p.run()
        update_manifest_status(user_id, session_id, "completed")      # ← NEW
        return result
    
    except Exception as exc:                                           # ← NEW
        update_manifest_status(                                        # ← NEW
            user_id, session_id, "failed", {"error": str(exc)}        # ← NEW
        )                                                              # ← NEW
        raise                                                          # ← NEW
>>>>>>> 6e0ca9d (added dvc in streaming and batch pipeline (let me know if i need to update test code))
