CREATE TABLE IF NOT EXISTS analytics.unique_top50_ranked_artist_count AS (
    SELECT unnested_artists_name -- Dashborad: COUNT(*) AS count
    FROM postgres_raw_raw.top50_track_information
);
