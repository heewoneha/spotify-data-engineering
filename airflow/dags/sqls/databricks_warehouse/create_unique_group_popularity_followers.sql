CREATE TABLE IF NOT EXISTS analytics.unique_group_popularity_followers AS (
    SELECT group_name, group_popularity, followers, gender,
    CAST(date_str AS DATE) AS record_date
    FROM postgres_raw_raw.daily_group_artist_spotify_popularity
);
