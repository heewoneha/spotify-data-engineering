CREATE TABLE IF NOT EXISTS "raw"."daily_top50_track" AS (
    SELECT DISTINCT track_id, track_name, track_number_in_playlist, year, month, day
    FROM "data_catalog"."top50_tracks_info"
);
