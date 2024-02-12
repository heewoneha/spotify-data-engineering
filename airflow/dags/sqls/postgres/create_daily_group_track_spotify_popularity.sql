CREATE TABLE IF NOT EXISTS "raw"."daily_group_track_spotify_popularity" AS (
    SELECT DISTINCT track_id, track_name, group_name, track_popularity, year, month, day
    FROM "data_catalog"."kpop_group_track_info"
);
