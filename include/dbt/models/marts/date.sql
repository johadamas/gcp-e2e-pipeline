-- models/marts/date.sql

{{
    config(
        materialized = "table", 
        schema = 'marts'
    )
}}

WITH date_data AS (
    SELECT
        DISTINCT publishedAt AS published_time
    FROM {{ source('youtube_tf', 'youtube_raw') }}  -- Reference the staging table
)

SELECT 
    DISTINCT published_time AS date_id,
    DATE(published_time) AS date,
    EXTRACT(YEAR FROM published_time) AS year,
    EXTRACT(MONTH FROM published_time) AS month,
    EXTRACT(DAY FROM published_time) AS day,
    EXTRACT(DAYOFWEEK FROM published_time) AS day_of_week, -- 1 (Sunday) to 7 (Saturday) in most SQL databases
    FORMAT_TIMESTAMP('%A', published_time) AS day_name, -- e.g., "Monday"
    FORMAT_TIMESTAMP('%B', published_time) AS month_name, -- e.g., "October"
    EXTRACT(WEEK FROM published_time) AS week_number -- Week number of the year
FROM date_data