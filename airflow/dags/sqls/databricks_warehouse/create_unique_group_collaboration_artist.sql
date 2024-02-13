CREATE TABLE IF NOT EXISTS analytics.unique_group_collaboration_artist AS (
    SELECT UGA.group_name, GTI.unnested_artists_name, GTI.track_id, GTI.track_name
    FROM postgres_raw_raw.group_track_information AS GTI
    JOIN analytics.unique_group_artist AS UGA
    ON GTI.track_id = UGA.track_id
    WHERE GTI.unnested_artists_name <> UGA.group_name
);
