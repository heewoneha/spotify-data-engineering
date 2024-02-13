CREATE TABLE IF NOT EXISTS analytics.unique_top50_artist_avg_audio_feature AS (
    SELECT
        TI.unnested_artists_name,
        AVG(AF.acousticness) AS avg_acousticness,
        AVG(AF.danceability) AS avg_danceability,
        AVG(AF.energy) AS avg_energy,
        AVG(AF.tempo) AS avg_tempo,
        AVG(AF.loudness) AS avg_loudness,
        AVG(AF.valence) AS avg_valence,
        AVG(AF.speechiness) AS avg_speechiness
    FROM postgres_raw_raw.top50_audio_feature AS AF
    JOIN postgres_raw_raw.top50_track_information AS TI
    ON AF.track_id = TI.track_id
    GROUP BY TI.unnested_artists_name
);
