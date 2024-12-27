-- models/report/average_video_duration.sql

{{
    config(
        materialized = "table", 
        schema = 'reports'
    )
}}

WITH channel_metrics AS (
    SELECT
        c.channel_id,
        c.channel_name,
        SUM(f.view_count) AS total_views,
        SUM(f.like_count) AS total_likes,
        SUM(f.comment_count) AS total_comments,
        COUNT(f.video_id) AS total_videos,
        AVG(v.duration_seconds) / 60 AS avg_duration_minutes  -- Convert seconds to minutes
    FROM {{ ref('metrics') }} f
    JOIN {{ ref('channel') }} c ON f.channel_id = c.channel_id
    JOIN {{ ref('videos') }} v ON f.video_id = v.video_id
    GROUP BY c.channel_id, c.channel_name
)

SELECT * FROM channel_metrics