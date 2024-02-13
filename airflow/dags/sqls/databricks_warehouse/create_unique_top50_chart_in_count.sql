CREATE TABLE IF NOT EXISTS analytics.unique_top50_chart_in_count AS (
    SELECT
        COUNT(track_number_in_playlist) AS chart_in_count,
        track_number_in_playlist,
        track_name,
        track_id
    FROM postgres_raw_raw.daily_top50_track
    GROUP BY track_number_in_playlist, track_name, track_id
);
