import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.textio import WriteToText
from backend.pipelines.api.auth import GoogleAuthClient
from backend.pipelines.api.youtube_client import YoutubeClient
from backend.pipelines.api import ReadFromAPI
from backend.pipelines.api.csv_writer import CSVWriter 
import os


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = os.environ['PROJECT_ID']
google_cloud_options.job_name = "youtube-pipeline"
google_cloud_options.region='us-central1'
google_cloud_options.staging_location = "gs://youtube-pipeline-staging-bucket/staging"
google_cloud_options.temp_location = "gs://youtube-pipeline-staging-bucket/temp"
options.view_as(StandardOptions).runner = "DataflowRunner"
google_cloud_options.service_account_email='serviceaccountforgithub@main-shade-485500-a0.iam.gserviceaccount.com'



def run():
    auth = GoogleAuthClient(
        token_uri=os.environ["TOKEN_URI"],
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
        redirect_uri=os.environ["REDIRECT_URIS"],
        refresh_token=os.environ["YOUTUBE_REFRESH_TOKEN"]
    )
    access_token=auth.get_access_token()
    yt_client=YoutubeClient(access_token)
    csv_client=CSVWriter()
    
    with beam.Pipeline(options=options) as p:
        header = ','.join(['playlist_name', 'track_title', 'artist_name', 'video_id', 'genre','country','collection_name','collection_id','trackTimeMillis', 'view_count','like_count','comment_count'])
        data=(
            p
            |'Seed'>>beam.Create([None])
            |'Read From API'>>beam.ParDo(ReadFromAPI(access_token))
            |'ToCSV' >> beam.Map(csv_client.dict_to_csv_line)
        )
        header_pcoll = (
            p | 'Header' >> beam.Create([header])
        )
        final_csv = (
            (header_pcoll, data) | beam.Flatten()
        )
        final_csv |'WriteToGCS'>> WriteToText(file_path_prefix='gs://youtube-pipeline-staging-bucket/Final_Output',file_name_suffix='.csv',shard_name_template='')


