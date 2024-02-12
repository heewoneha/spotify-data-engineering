CREATE TABLE IF NOT EXISTS "raw"."top50_audio_feature" AS (
    SELECT DISTINCT A.track_id, I.track_name, A.acousticness, A.danceability,
                    A.energy, A.loudness, A.speechiness, A.tempo, A.valence
    FROM  "data_catalog"."top50_audio_features" AS A, "data_catalog"."top50_tracks_info" AS I
    WHERE A.track_id = I.track_id
);
