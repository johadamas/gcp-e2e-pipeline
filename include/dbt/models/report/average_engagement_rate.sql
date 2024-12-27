-- models/report/average_engagement_rate.sql

{{
    config(
        materialized = "table", 
        schema = 'reports'
    )
}}

WITH engagement_metrics AS (
    SELECT
        c.channel_id,
        c.channel_name,
        f.video_id,
        f.view_count,
        f.like_count,
        f.comment_count,
        CAST(f.date_id AS DATE) AS date_id,  -- Cast to DATE for filtering
        CASE 
            WHEN f.view_count > 0 THEN (f.like_count + f.comment_count) / f.view_count
            ELSE 0  -- Handle videos with zero views to avoid division by zero
        END AS engagement_rate
    FROM {{ ref('metrics') }} f
    JOIN {{ ref('channel') }} c ON f.channel_id = c.channel_id
    WHERE 
        DATE_TRUNC(CAST(f.date_id AS DATE), MONTH) < DATE_TRUNC(CURRENT_DATE(), MONTH)  -- Exclude videos from the current month
        AND CAST(f.date_id AS DATE) >= DATE('2019-01-01')  -- Only include videos from 01/01/2019 onwards
),

average_engagement_by_channel AS (
    SELECT
        channel_id,
        channel_name,
        AVG(engagement_rate) AS avg_engagement_rate
    FROM engagement_metrics
    GROUP BY channel_id, channel_name
)

SELECT *
FROM average_engagement_by_channel
ORDER BY avg_engagement_rate DESC
