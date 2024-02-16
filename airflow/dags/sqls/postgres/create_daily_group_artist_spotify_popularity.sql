CREATE TABLE IF NOT EXISTS "raw"."daily_group_artist_spotify_popularity" AS (
    SELECT DISTINCT group_id, group_name, followers, group_popularity, gender,
    CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) AS date_str
    FROM   "data_catalog"."kpop_group_artist_info"
);
