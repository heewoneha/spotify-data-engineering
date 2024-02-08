CREATE TABLE data_catalog.top50_audio_features(
    track_id VARCHAR(50) NOT NULL,
    acousticness double precision NOT NULL,
    danceability double precision NOT NULL,
    duration_ms INT NOT NULL,
    energy double precision NOT NULL,
    instrumentalness double precision NOT NULL,
    key INT NOT NULL,
    liveness double precision NOT NULL,
    loudness double precision NOT NULL,
    mode INT NOT NULL,
    speechiness double precision NOT NULL,
    tempo double precision NOT NULL,
    time_signature INT NOT NULL,
    valence double precision NOT NULL,
    year CHAR(4) NOT NULL,
    month CHAR(2) NOT NULL,
    day CHAR(2) NOT NULL
);
