from fastapi import HTTPException, APIRouter
import psycopg2
import os

router = APIRouter(prefix='/api/v2')

host = os.getenv('SERVER_NAME')
db_name = os.getenv('DATABASE_NAME')
user = os.getenv('ADMIN_USER_NAME')
password = os.getenv('ADMIN_PASSWORD')
ssl_mode = os.getenv('SSL_MODE')
schema_name = os.getenv('SCHEMA_NAME')


@router.get('/data-factory-http-query')
def execute_data_factory_data_catalog_query():
    try:
        conn_string = "host={0} user={1} dbname={2} password={3} " \
            "sslmode={4} options='-c search_path={5}'".format(host, user, db_name, password, ssl_mode, schema_name)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS top50_tracks_info, top50_audio_features, kpop_boy_group_artist_info, \
                    kpop_girl_group_artist_info, kpop_boy_group_track_info, kpop_girl_group_track_info;")

        cursor.execute(
            "CREATE TABLE data_catalog.top50_tracks_info( \
                track_id VARCHAR(50) NOT NULL, \
                track_name VARCHAR(255) NOT NULL, \
                artists_name VARCHAR(255) NOT NULL, \
                album_release_date CHAR(10) NOT NULL, \
                track_number_in_playlist INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"
        )

        cursor.execute(
            "CREATE TABLE data_catalog.top50_audio_features( \
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
        )

        cursor.execute(
            "CREATE TABLE data_catalog.kpop_boy_group_artist_info( \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                followers INT NOT NULL, \
                group_genres VARCHAR(255) NOT NULL, \
                group_popularity INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"
        )

        cursor.execute(
            "CREATE TABLE data_catalog.kpop_girl_group_artist_info( \
                group_id VARCHAR(50) NOT NULL, \
                group_name VARCHAR(20) NOT NULL, \
                followers INT NOT NULL, \
                group_genres VARCHAR(255) NOT NULL, \
                group_popularity INT NOT NULL, \
                year CHAR(4) NOT NULL, \
                month CHAR(2) NOT NULL, \
                day CHAR(2) NOT NULL \
            );"
        )

        cursor.execute(
            "CREATE TABLE data_catalog.kpop_boy_group_track_info( \
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
        )

        cursor.execute(
            "CREATE TABLE data_catalog.kpop_girl_group_track_info( \
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
        )

        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "Success"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
