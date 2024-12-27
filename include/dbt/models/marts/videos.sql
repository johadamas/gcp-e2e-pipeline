-- models/marts/videos.sql

{{
    config(
        materialized = "table", 
        schema = 'marts'
    )
}}

WITH video_data AS (
    SELECT 
        DISTINCT 
        video_id,
        title AS video_title,
        tags,
        duration,
        -- Extract minutes and seconds from the ISO 8601 duration format
        CAST(REGEXP_EXTRACT(duration, r'PT(\d+)M') AS INT64) AS minutes,
        CAST(REGEXP_EXTRACT(duration, r'(\d+)S') AS INT64) AS seconds
    FROM {{ source('youtube_tf', 'youtube_raw') }}  -- Reference to your staging model or table
),

converted_video_data AS (
    SELECT 
        video_id,
        video_title,
        tags,
        -- Calculate duration in minutes and seconds
        COALESCE(minutes, 0) + COALESCE(seconds, 0) / 60.0 AS duration_minutes,
        COALESCE(minutes, 0) * 60 + COALESCE(seconds, 0) AS duration_seconds
    FROM video_data
)

SELECT * 
FROM converted_video_data
