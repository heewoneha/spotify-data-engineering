CREATE TABLE IF NOT EXISTS analytics.unique_group_track_popularity AS (
    SELECT DISTINCT track_id, track_name, group_name, track_popularity -- Dashboard: avg(track_popularity)
    FROM postgres_raw_raw.daily_group_track_spotify_popularity
);
