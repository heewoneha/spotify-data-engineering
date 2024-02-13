CREATE TABLE IF NOT EXISTS "raw"."daily_group_track_spotify_popularity" AS (
    SELECT DISTINCT track_id, track_name, group_name, track_popularity,
    CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) AS date_str
    FROM "data_catalog"."kpop_group_track_info"
);
