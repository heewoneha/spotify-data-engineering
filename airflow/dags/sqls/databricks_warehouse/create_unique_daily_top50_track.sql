CREATE TABLE IF NOT EXISTS analytics.unique_daily_top50_track AS (
    SELECT
        CAST(date_str AS DATE) AS record_date,
        track_number_in_playlist,
        track_id,
        track_name,
        track_url
    FROM postgres_raw_raw.daily_top50_track
);