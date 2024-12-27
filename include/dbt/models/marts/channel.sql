-- models/marts/channel.sql

{{
    config(
        materialized = "table", 
        schema = 'marts'
    )
}}

WITH channel_data AS (
    SELECT 
        DISTINCT channelId AS channel_id,
        channel_name, 
        SAFE_CAST(subscribers AS INT64) AS total_subscribers,
        SAFE_CAST(total_views AS INT64) AS total_views,
        SAFE_CAST(total_videos AS INT64) AS total_videos
    FROM {{ source('youtube_tf', 'youtube_raw') }}  -- Reference to your staging model or table
)

SELECT * FROM channel_data