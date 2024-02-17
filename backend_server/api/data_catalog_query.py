from fastapi import HTTPException, APIRouter
import psycopg2
import os
from api.dbkit import TOP50_TRACKS_INFO_TABLE_CREATE_SQL, \
    TOP50_AUDIO_FEATURES_TABLE_CREATE_SQL, \
    KPOP_BOY_GROUP_ARTIST_INFO_TABLE_CREATE_SQL, \
    KPOP_GIRL_GROUP_ARTIST_INFO_TABLE_CREATE_SQL, \
    KPOP_BOY_GROUP_TRACK_INFO_TABLE_CREATE_SQL, \
    KPOP_GIRL_GROUP_TRACK_INFO_TABLE_CREATE_SQL, \
    KPOP_GROUP_ARTIST_INFO_TABLE_CREATE_SQL, \
    KPOP_GROUP_TRACK_INFO_TABLE_CREATE_SQL


router = APIRouter(prefix='/api/v2')

host = os.getenv('SERVER_NAME')
db_name = os.getenv('DATABASE_NAME')
user = os.getenv('ADMIN_USER_NAME')
password = os.getenv('ADMIN_PASSWORD')
ssl_mode = os.getenv('SSL_MODE')
schema_name = os.getenv('SCHEMA_NAME')


@router.get('/data-factory-http-scraper-query')
def execute_data_factory_scraper_data_catalog_query():
    try:
        conn_string = "host={0} user={1} dbname={2} password={3} " \
            "sslmode={4} options='-c search_path={5}'".format(host, user, db_name, password, ssl_mode, schema_name)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS top50_tracks_info, top50_audio_features, kpop_boy_group_artist_info, \
                    kpop_girl_group_artist_info, kpop_boy_group_track_info, kpop_girl_group_track_info;")

        cursor.execute(TOP50_TRACKS_INFO_TABLE_CREATE_SQL)

        cursor.execute(TOP50_AUDIO_FEATURES_TABLE_CREATE_SQL)

        cursor.execute(KPOP_BOY_GROUP_ARTIST_INFO_TABLE_CREATE_SQL)

        cursor.execute(KPOP_GIRL_GROUP_ARTIST_INFO_TABLE_CREATE_SQL)

        cursor.execute(KPOP_BOY_GROUP_TRACK_INFO_TABLE_CREATE_SQL)

        cursor.execute(KPOP_GIRL_GROUP_TRACK_INFO_TABLE_CREATE_SQL)

        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "Success scraper data catalog"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/data-factory-http-preprocessing-query')
def execute_data_factory_preprocessing_data_catalog_query():
    try:
        conn_string = "host={0} user={1} dbname={2} password={3} " \
            "sslmode={4} options='-c search_path={5}'".format(host, user, db_name, password, ssl_mode, schema_name)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS top50_tracks_info, top50_audio_features, \
                    kpop_group_artist_info, kpop_group_track_info;")

        cursor.execute(TOP50_TRACKS_INFO_TABLE_CREATE_SQL)

        cursor.execute(TOP50_AUDIO_FEATURES_TABLE_CREATE_SQL)

        cursor.execute(KPOP_GROUP_ARTIST_INFO_TABLE_CREATE_SQL)

        cursor.execute(KPOP_GROUP_TRACK_INFO_TABLE_CREATE_SQL)

        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "Success preprocessing data catalog"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
