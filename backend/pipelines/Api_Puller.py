import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.textio import WriteToText
from backend.pipelines.api.auth import GoogleAuthClient
from backend.pipelines.api.youtube_client import YoutubeClient
from backend.pipelines.api.ReadFromAPI import ReadFromAPI
from backend.pipelines.api.csv_writer import dict_to_csv_line 
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.dataflow import DataflowRunner
import os


def run_pipeline_for_user(user_id,refresh_token,gcs_prefix):
    auth = GoogleAuthClient(
        token_uri=os.environ["TOKEN_URI"],
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
        redirect_uri=os.environ["REDIRECT_URIS"],
        refresh_token=refresh_token
    )
    access_token=auth.get_access_token()
    yt_client=YoutubeClient(access_token)
    columns=['playlist_name', 'track_title', 'artist_name', 'video_id', 'genre','country','collection_name','collection_id','trackTimeMillis', 'view_count','like_count','comment_count']


    options = PipelineOptions(["--setup_file=./setup.py"])
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = os.environ['PROJECT_ID']
    google_cloud_options.job_name = f"youtube-pipeline--{user_id}"
    google_cloud_options.region='us-central1'
    google_cloud_options.staging_location = "gs://youtube-pipeline-staging-bucket/staging"
    google_cloud_options.temp_location = "gs://youtube-pipeline-staging-bucket/temp"
    options.view_as(StandardOptions).runner = "DataflowRunner"
    google_cloud_options.service_account_email='serviceaccountforgithub@main-shade-485500-a0.iam.gserviceaccount.com'
    
    p=beam.Pipeline(options=options)
    (
            p
            |'Seed'>>beam.Create([None])
            |'Read From API'>>beam.ParDo(ReadFromAPI(access_token))
            |'ToCSV' >> beam.Map(lambda r: dict_to_csv_line(r, columns))
            |'WriteToGCS'>> WriteToText(file_path_prefix=f'gs://youtube-pipeline-staging-bucket/{gcs_prefix}{user_id}',file_name_suffix='.csv',shard_name_template='',header=','.join(columns))
    )
    result=p.run()
    result.wait_until_finish()
