import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

@dag(
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Adjust schedule as per your needs
    catchup=False,
    tags=['DE_PROJECT3'],
)
def youtube_pipeline():

    # Fetch the API key and channel IDs from Airflow variables
    api_key = Variable.get('YT_API_KEY')
    channel_miawaug = Variable.get('MiawAug')
    channel_windahb = Variable.get('WindahB')

    # Build the YouTube API client
    youtube = build('youtube', 'v3', developerKey=api_key)

    # Use the retrieved channel IDs
    channel_id = [channel_miawaug, channel_windahb]

    # Task to fetch YouTube channel stats and video details
    @task()
    def get_youtube_data():
        def get_channel_stats(youtube, channel_id):
            request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id
            )
            response = request.execute()

            all_data = []
            for item in response['items']:
                data = {
                    'channel_name': item['snippet']['title'],
                    'subscribers': item['statistics']['subscriberCount'],
                    'total_views': item['statistics']['viewCount'],
                    'total_videos': item['statistics']['videoCount'],
                    'playlistId': item['contentDetails']['relatedPlaylists']['uploads'],
                    'channelId': item['id']  # Add channelId for merging
                }
                all_data.append(data)
            return all_data

        def get_video_ids(youtube, playlist_ids):
            video_ids = []
            for playlist_id in playlist_ids:
                request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=playlist_id,
                    maxResults=50
                )
                response = request.execute()
                for item in response['items']:
                    video_ids.append(item['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')
                while next_page_token:
                    request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    for item in response['items']:
                        video_ids.append(item['contentDetails']['videoId'])
                    next_page_token = response.get('nextPageToken')
            return video_ids

        def get_video_details(youtube, all_video_ids):
            all_video_info = []
            for i in range(0, len(all_video_ids), 50):
                request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=','.join(all_video_ids[i:i+50])
                )
                response = request.execute()
                for video in response['items']:
                    stats_to_keep = {
                        'snippet': ['title', 'tags', 'publishedAt', 'channelId', 'channelTitle'],
                        'statistics': ['viewCount', 'likeCount', 'commentCount'],
                        'contentDetails': ['duration']
                    }
                    video_info = {'video_id': video['id']}
                    for part in stats_to_keep:
                        for field in stats_to_keep[part]:
                            video_info[field] = video.get(part, {}).get(field, None)
                    all_video_info.append(video_info)
            return pd.DataFrame(all_video_info)

        # Fetch channel data and video IDs
        channel_data = get_channel_stats(youtube, channel_id)
        channel_df = pd.DataFrame(channel_data)
        playlist_ids = channel_df['playlistId'].tolist()
        all_video_ids = get_video_ids(youtube, playlist_ids)

        # Fetch video details
        video_df = get_video_details(youtube, all_video_ids)

        # Drop rows where video_id is '4vkG-H2CB6E' (VIDEO ERROR BANG WINDAH!!)
        video_df = video_df[video_df['video_id'] != '4vkG-H2CB6E']

        # Merge video data with channel data on 'channelId'
        final_df = pd.merge(video_df, channel_df, on='channelId', how='left')

        # Save final dataset to CSV
        final_df.to_csv('/tmp/youtube_data.csv', index=False)

    # Create a task group for Terraform tasks
    with TaskGroup("terraform_tasks", tooltip="Tasks for Terraform operations") as terraform_group:

        # Initialize Terraform
        terraform_init = BashOperator(
            task_id="terraform_init",
            bash_command="terraform -chdir=/usr/local/airflow/include/terraform init"
        )

        # Apply Terraform
        terraform_apply = BashOperator(
            task_id="terraform_apply",
            bash_command="terraform -chdir=/usr/local/airflow/include/terraform apply -auto-approve"
        )

    # Task: Upload CSV file to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src='/tmp/youtube_data.csv',
        dst="data/youtube_data.csv",
        bucket='testproject-bq-02-tfbucket',  # Replace with your GCS bucket
        gcp_conn_id='gcp',
    )

    # Task: Load data from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_data_to_bq',
        bucket='testproject-bq-02-tfbucket',
        source_objects=['data/youtube_data.csv'],
        destination_project_dataset_table='testproject-bq-02.youtube_tf.youtube_raw',  # Replace with project_id, dataset, table name
        schema_fields=[
            {'name': 'video_id', 'type': 'STRING'},
            {'name': 'title', 'type': 'STRING'},
            {'name': 'tags', 'type': 'STRING'},
            {'name': 'publishedAt', 'type': 'TIMESTAMP'},
            {'name': 'channelId', 'type': 'STRING'},
            {'name': 'channelTitle', 'type': 'STRING'},
            {'name': 'viewCount', 'type': 'INT64'},
            {'name': 'likeCount', 'type': 'INT64'},
            {'name': 'commentCount', 'type': 'INT64'},
            {'name': 'duration', 'type': 'STRING'},
            {'name': 'channel_name', 'type': 'STRING'},       # New column
            {'name': 'subscribers', 'type': 'INT64'},         # New column
            {'name': 'total_views', 'type': 'INT64'},         # New column
            {'name': 'total_videos', 'type': 'INT64'},        # New column
            {'name': 'playlistId', 'type': 'STRING'},         # New column
        ],
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='gcp',
    )

    #dbt transform
    transform_marts = DbtTaskGroup(
        group_id='transform_marts', 
        project_config=DBT_PROJECT_CONFIG, 
        profile_config=DBT_CONFIG, 
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, 
            select=['path:models/marts']
        )
    )

    #dbt transform_report
    transform_report = DbtTaskGroup(
        group_id='transform_report', 
        project_config=DBT_PROJECT_CONFIG, 
        profile_config=DBT_CONFIG, 
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, 
            select=['path:models/report']
        )
    )

    # Task dependencies
    youtube_data = get_youtube_data()
    youtube_data >> terraform_group >> upload_to_gcs >> load_to_bigquery >> transform_marts >> transform_report

# Instantiate the DAG
youtube_pipeline = youtube_pipeline()
