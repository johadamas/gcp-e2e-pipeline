-- models/marts/metrics.sql

{{
    config(
        materialized = "table", 
        schema = 'marts'
    )
}}

WITH youtube_data AS (
    SELECT
        video_id,
        publishedAt AS date_id,  -- Foreign key for dim_date
        channelId AS channel_id,
        viewCount AS view_count,
        likeCount AS like_count,
        commentCount AS comment_count
    FROM {{ source('youtube_tf', 'youtube_raw') }}
)

SELECT
    video_id,        -- Foreign key for dim_videos
    date_id,         -- Foreign key for dim_date
    channel_id,      -- Foreign key for dim_channels
    COALESCE(view_count, 0) AS view_count,
    COALESCE(like_count, 0) AS like_count,
    COALESCE(comment_count, 0) AS comment_count
FROM youtube_data
