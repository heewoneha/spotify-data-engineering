CREATE TABLE IF NOT EXISTS analytics.unique_top50_artist_avg_audio_feature AS (
    SELECT
        TI.unnested_artists_name,
        AF.acousticness, -- Dashborad: avg(acousticness)
        AF.danceability, -- Dashborad: avg(danceability)
        AF.energy, -- Dashborad: avg(energy)
        AF.tempo, -- Dashborad: avg(tempo)
        AF.loudness, -- Dashborad: avg(loudness)
        AF.valence, -- Dashborad: avg(valence)
        AF.speechiness -- Dashborad: avg(speechiness)
    FROM postgres_raw_raw.top50_audio_feature AS AF
    JOIN postgres_raw_raw.top50_track_information AS TI
    ON AF.track_id = TI.track_id
);
