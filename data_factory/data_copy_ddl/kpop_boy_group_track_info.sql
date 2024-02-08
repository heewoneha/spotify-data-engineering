CREATE TABLE data_catalog.kpop_boy_group_track_info(
    track_id VARCHAR(50) NOT NULL,
    track_name VARCHAR(255) NOT NULL,
    group_id VARCHAR(50) NOT NULL,
    group_name VARCHAR(20) NOT NULL,
    artists_name VARCHAR(255) NOT NULL,
    album_release_date CHAR(10) NOT NULL,
    track_popularity INT NOT NULL,
    duration_ms INT NOT NULL,
    year CHAR(4) NOT NULL,
    month CHAR(2) NOT NULL,
    day CHAR(2) NOT NULL
);
