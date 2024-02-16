CREATE TABLE IF NOT EXISTS "raw"."daily_top50_track" AS (
    SELECT DISTINCT track_id, track_name, track_number_in_playlist,
    'https://open.spotify.com/track/' || track_id AS track_url,
    CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) AS date_str
    FROM "data_catalog"."top50_tracks_info"
);
