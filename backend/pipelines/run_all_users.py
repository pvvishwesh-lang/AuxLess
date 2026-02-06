import os
from backend.pipelines.api.firestore_client import FirestoreClient
from backend.pipelines.Api_Puller import run_pipeline_for_user
from google.cloud import storage

def combine_gcs_files(bucket_name,input_prefix,output_file):
    client=storage.Client()
    bucket=client.bucket(bucket_name)
    blobs=bucket.list_blobs(prefix=input_prefix)
    combined_lines=[]
    for blob in blobs:
        if blob.name.endswith('.csv'):
            combined_lines.extend(blob.download_as_text().splitlines())

    header=combined_lines[0]
    body=[line for line in combined_lines if line!=header]
    combined_content='\n'.join([header]+body)
    output_blob=bucket.blob(output_file)
    output_blob.upload_from_string(combined_content)
    print(f'File saved to gs://{bucket_name}/{output_file}')

def main():
    project_id=os.environ['PROJECT_ID']
    bucket_name='youtube-pipeline-staging-bucket'
    temp_prefix='user_outputs/'
    final_output='Final_Output/combined_playlist.csv'
    fs_client=FirestoreClient(project_id)
    users=fs_client.get_all_users()
    for user_id,refresh_token in users:
        print(f'Running pipeline for {user_id}')
        run_pipeline_for_user(user_id,refresh_token,temp_prefix)
    combine_gcs_files(bucket_name,temp_prefix,final_output)

if __name__=='__main__':
    main()
