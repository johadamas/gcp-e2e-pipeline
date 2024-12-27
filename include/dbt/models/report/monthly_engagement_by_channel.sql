-- models/report/monthly_engagement_by_channel.sql

{{
    config(
        materialized = "table", 
        schema = 'reports'
    )
}}

WITH monthly_data AS (
    SELECT
        c.channel_id,
        c.channel_name,
        DATE_TRUNC(d.date, MONTH) AS month,  -- Adjust to WEEK for weekly trends
        SUM(f.view_count) AS total_monthly_views,
        SUM(f.like_count) AS total_monthly_likes,
        SUM(f.comment_count) AS total_monthly_comments
    FROM {{ ref('metrics') }} f
    JOIN {{ ref('channel') }} c ON f.channel_id = c.channel_id
    JOIN {{ ref('date') }} d ON f.date_id = d.date_id
    GROUP BY c.channel_id, c.channel_name, month
),

monthly_engagement_rate AS (
    SELECT
        channel_name,
        month,
        CASE 
            WHEN total_monthly_views > 0 THEN (total_monthly_likes + total_monthly_comments) / total_monthly_views
            ELSE 0  -- Handle cases where total views are zero
        END AS engagement_rate_by_views
    FROM monthly_data
)

SELECT *
FROM monthly_engagement_rate
ORDER BY month, channel_name