CREATE TABLE IF NOT EXISTS "raw"."group_track_information" AS (
    SELECT DISTINCT track_id, track_name, album_release_date, duration_ms,
           UNNEST(string_to_array(regexp_replace(artists_name, '[\[\]"]', '', 'g'), ',')) AS unnested_artists_name
    FROM "data_catalog"."kpop_group_track_info"
);
