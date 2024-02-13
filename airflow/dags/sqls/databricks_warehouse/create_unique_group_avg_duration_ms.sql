CREATE TABLE IF NOT EXISTS analytics.unique_group_avg_duration_ms AS (
    SELECT UGA.group_name, AVG(GTI.duration_ms)
    FROM postgres_raw_raw.group_track_information AS GTI
    JOIN analytics.unique_group_artist AS UGA
    ON GTI.track_id = UGA.track_id
    GROUP BY UGA.group_name
);