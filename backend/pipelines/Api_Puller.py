import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.textio import WriteToText
from backend.pipelines.api.auth import GoogleAuthClient
from backend.pipelines.api.youtube_client import YoutubeClient
from backend.pipelines.api.beam_extensions import ValidatingDoFn
from backend.pipelines.api.csv_writer import dict_to_csv_line 
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.dataflow import DataflowRunner
import os
import re
from datetime import datetime
import uuid
import json
from backend.pipelines.api.ReadFromAPI import ReadFromAPI


def sanitize_for_job_name(s: str) -> str:
    s = s.lower() 
    s = re.sub(r'[^a-z0-9-]', '-', s) 
    s = re.sub(r'-+', '-', s)
    s = s.strip('-')
    if not s[0].isalpha():
        s = 'a' + s
    if not s[-1].isalnum():
        s = s + '1'
    return s

def run_pipeline_for_user(user_id,refresh_token,prefix_valid,prefix_invalid,session_id):
    auth = GoogleAuthClient(
        token_uri=os.environ["TOKEN_URI"],
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
        redirect_uri=os.environ["REDIRECT_URIS"],
        refresh_token=refresh_token
    )
    unique_id=str(uuid.uuid4())[:4]
    access_token=auth.get_access_token()
    yt_client=YoutubeClient(access_token)
    columns=['playlist_name', 'track_title', 'artist_name', 'video_id', 'genre','country','collection_name','collection_id','trackTimeMillis', 'view_count','like_count','comment_count','trackTimeSeconds','like_to_view_ratio','comment_to_view_ratio']
    options = PipelineOptions(["--setup_file=./setup.py"])
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = os.environ['PROJECT_ID']
    google_cloud_options.job_name = f"youtube-pipeline-{session_id[:10]}-{sanitize_for_job_name(user_id)[:10]}-{unique_id}"
    google_cloud_options.region='us-central1'
    google_cloud_options.staging_location = "gs://youtube-pipeline-staging-bucket/staging"
    google_cloud_options.temp_location = "gs://youtube-pipeline-staging-bucket/temp"
    options.view_as(StandardOptions).runner = "DataflowRunner"
    google_cloud_options.service_account_email='serviceaccountforgithub@main-shade-485500-a0.iam.gserviceaccount.com'
    valid_blob_path = f"{prefix_valid}/{user_id}_valid.csv"
    invalid_blob_path = f"{prefix_invalid}/{user_id}_invalid.csv"
    p=beam.Pipeline(options=options)
    validated = (
        p
        | 'Seed' >> beam.Create([None])
        | 'Read From API' >> beam.ParDo(ReadFromAPI(access_token))
        | 'Validate records' >> beam.ParDo(ValidatingDoFn(access_token,session_id=session_id,user_id=user_id)).with_outputs("invalid_records", main="valid")
    )
    valid_records = validated.valid
    invalid_records = validated.invalid_records
    (
        valid_records
        | 'Valid To CSV' >> beam.Map(lambda r: dict_to_csv_line(r, columns))
        | 'Write Valid To GCS' >> WriteToText(file_path_prefix=f'gs://{valid_blob_path}{user_id}',file_name_suffix='.csv',shard_name_template='',header=','.join(columns))
    )
    (
        invalid_records
        | 'Invalid To CSV' >> beam.Map(lambda r: f'{r["error"]},"{json.dumps(r["record"])}"')
        | 'Write Invalid To GCS' >> WriteToText(file_path_prefix=f'gs://{invalid_blob_path}{user_id}',file_name_suffix='.csv',shard_name_template='',header='error,record_json')
    )
    return p.run()
