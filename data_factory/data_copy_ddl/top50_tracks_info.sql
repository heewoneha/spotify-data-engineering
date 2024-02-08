CREATE TABLE data_catalog.top50_tracks_info(
    track_id VARCHAR(50) NOT NULL,
    track_name VARCHAR(255) NOT NULL,
    artists_name VARCHAR(255) NOT NULL,
    album_release_date CHAR(10) NOT NULL,
    track_number_in_playlist INT NOT NULL,
    year CHAR(4) NOT NULL,
    month CHAR(2) NOT NULL,
    day CHAR(2) NOT NULL
);
