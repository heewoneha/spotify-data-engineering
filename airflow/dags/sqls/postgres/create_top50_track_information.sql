CREATE TABLE IF NOT EXISTS "raw"."top50_track_information" AS (
    SELECT DISTINCT track_id, track_name, album_release_date,
           UNNEST(string_to_array(regexp_replace(artists_name, '[\[\]"]', '', 'g'), ',')) AS unnested_artists_name
    FROM "data_catalog"."top50_tracks_info"
);
