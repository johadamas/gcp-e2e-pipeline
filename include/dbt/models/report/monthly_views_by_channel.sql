-- models/report/monthly_views_by_channel.sql

{{
    config(
        materialized = "table", 
        schema = 'reports'
    )
}}

WITH monthly_views AS (
    SELECT
        c.channel_id,
        c.channel_name,
        DATE_TRUNC(d.date, MONTH) AS month,  -- Aggregates data by month
        SUM(f.view_count) AS total_monthly_views
    FROM {{ ref('metrics') }} f
    JOIN {{ ref('channel') }} c ON f.channel_id = c.channel_id
    JOIN {{ ref('date') }} d ON f.date_id = d.date_id
    GROUP BY c.channel_id, c.channel_name, month
)

SELECT 
    channel_name,
    month,
    total_monthly_views
FROM monthly_views
ORDER BY month, channel_name