TOP50_TRACKS_INFO_TABLE_CREATE_SQL = "CREATE TABLE data_catalog.top50_tracks_info( \
                track_id VARCHAR(50) NOT NULL, \
                track_name VARCHAR(255) NOT NULL, \
                artists_name VARCHAR(255) NOT NULL, \
                album_release_date CHAR(10) NOT NULL, \
                track_number_in_playlist INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

TOP50_AUDIO_FEATURES_TABLE_CREATE_SQL = "CREATE TABLE data_catalog.top50_audio_features( \
                track_id VARCHAR(50) NOT NULL, \
                acousticness double precision NOT NULL, \
                danceability double precision NOT NULL, \
                duration_ms INT NOT NULL, \
                energy double precision NOT NULL, \
                instrumentalness double precision NOT NULL, \
                key INT NOT NULL, \
                liveness double precision NOT NULL, \
                loudness double precision NOT NULL, \
                mode INT NOT NULL, \
                speechiness double precision NOT NULL, \
                tempo double precision NOT NULL, \
                time_signature INT NOT NULL, \
                valence double precision NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

KPOP_BOY_GROUP_ARTIST_INFO_TABLE_CREATE_SQL ="CREATE TABLE data_catalog.kpop_boy_group_artist_info( \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                followers INT NOT NULL, \
                group_genres VARCHAR(255) NOT NULL, \
                group_popularity INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

KPOP_GIRL_GROUP_ARTIST_INFO_TABLE_CREATE_SQL = "CREATE TABLE data_catalog.kpop_girl_group_artist_info( \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                followers INT NOT NULL, \
                group_genres VARCHAR(255) NOT NULL, \
                group_popularity INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

KPOP_BOY_GROUP_TRACK_INFO_TABLE_CREATE_SQL = "CREATE TABLE data_catalog.kpop_boy_group_track_info( \
                track_id VARCHAR(50) NOT NULL, \
                track_name VARCHAR(255) NOT NULL, \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                artists_name VARCHAR(255) NOT NULL, \
                album_release_date CHAR(10) NOT NULL, \
                track_popularity INT NOT NULL, \
                duration_ms INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

KPOP_GIRL_FROUP_TRACK_INFO_TABLE_CREATE_SQL ="CREATE TABLE data_catalog.kpop_girl_group_track_info( \
                track_id VARCHAR(50) NOT NULL, \
                track_name VARCHAR(255) NOT NULL, \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                artists_name VARCHAR(255) NOT NULL, \
                album_release_date CHAR(10) NOT NULL, \
                track_popularity INT NOT NULL, \
                duration_ms INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

KPOP_GROUP_ARTIST_INFO_TABLE_CREATE_SQL="CREATE TABLE data_catalog.kpop_group_artist_info( \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                followers INT NOT NULL, \
                group_genres VARCHAR(255) NOT NULL, \
                group_popularity INT NOT NULL, \
                gender INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

KPOP_GROUP_TRACK_INFO_TABLE_CREATE_SQL="CREATE TABLE data_catalog.kpop_group_track_info( \
                track_id VARCHAR(50) NOT NULL, \
                track_name VARCHAR(255) NOT NULL, \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                artists_name VARCHAR(255) NOT NULL, \
                album_release_date CHAR(10) NOT NULL, \
                track_popularity INT NOT NULL, \
                duration_ms INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"

