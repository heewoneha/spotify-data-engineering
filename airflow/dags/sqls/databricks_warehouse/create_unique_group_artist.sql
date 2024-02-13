CREATE TABLE IF NOT EXISTS analytics.unique_group_artist AS (
    SELECT DISTINCT group_name, track_id, track_name
    FROM postgres_raw_raw.daily_group_track_spotify_popularity
);
