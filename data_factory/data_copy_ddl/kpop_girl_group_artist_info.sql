CREATE TABLE data_catalog.kpop_girl_group_artist_info(
    group_id VARCHAR(50) NOT NULL,
    group_name VARCHAR(20) NOT NULL,
    followers INT NOT NULL,
    group_genres VARCHAR(255) NOT NULL,
    group_popularity INT NOT NULL,
    year CHAR(4) NOT NULL,
    month CHAR(2) NOT NULL,
    day CHAR(2) NOT NULL
);
