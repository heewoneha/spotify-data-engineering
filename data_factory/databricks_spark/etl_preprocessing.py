from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os


spark = SparkSession.builder \
    .appName('Pyspark preprocessing') \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

AZURE_BLOB_ACCOUNT_NAME = os.getenv('AZURE_BLOB_ACCOUNT_NAME')
AZURE_BLOB_ACCOUNT_KEY = os.getenv('AZURE_BLOB_ACCOUNT_KEY')
AZURE_BLOB_CONTAINER_NAME = os.getenv('AZURE_BLOB_CONTAINER_NAME')


class BlobUploader:
    def __init__(self, title, account_name, account_key, container_name):
      self.account_name = account_name
      self.account_key = account_key
      self.container_name = container_name
      self.title = title
    
    def save_parquet_to_blob(self, df):
      parquet_path = f"/mnt/blobstorage/{self.title}"
      df.write.mode('overwrite').parquet(parquet_path)
    
    def delete_trash_files(self):
      directory_path = f"/mnt/blobstorage/{self.title}"
      files = dbutils.fs.ls(directory_path)
      prefixes_to_delete = ['_committed_', '_started_', '_SUCCESS']
      
      for file_info in files:
          file_path = file_info.path
          if any(file_path.startswith('dbfs:'+directory_path+'/'+prefix) for prefix in prefixes_to_delete):
              dbutils.fs.rm(file_path)


def create_df_from_catalog(table_name):
    driver = "org.postgresql.Driver"
    database_host = os.getenv('SERVER_NAME')
    database_name = os.getenv('DATABASE_NAME')
    schema = os.getenv('SCHEMA_NAME')
    user = os.getenv('ADMIN_USER_NAME')
    password = os.getenv('DB_PASSWORD')
    database_port = '5432'
    url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"
    
    df = (spark.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", f'{schema}.{table_name}')
        .option("user", user)
        .option("password", password)
        .load()
    )
    return df

def save_df(title, df):
    blob = BlobUploader(
        title,
        AZURE_BLOB_ACCOUNT_NAME,
        AZURE_BLOB_ACCOUNT_KEY,
        AZURE_BLOB_CONTAINER_NAME
    )

    dbutils.fs.mount(
        source = f"wasbs://{blob.container_name}@{blob.account_name}.blob.core.windows.net/",
        mount_point = "/mnt/blobstorage",
        extra_configs = {"fs.azure.account.key." + blob.account_name + ".blob.core.windows.net": blob.account_key}
    )

    blob.save_parquet_to_blob(df)
    blob.delete_trash_files()

    dbutils.fs.unmount("/mnt/blobstorage")


if __name__ == '__main__':
    # top 50 tracks info
    top50_tracks_info_df = create_df_from_catalog('top50_tracks_info')
    title = 'preprocessed_top50_tracks_info'
    save_df(title, top50_tracks_info_df)

    # top 50 tracks features
    top50_audio_features_df = create_df_from_catalog('top50_audio_features')
    title = 'preprocessed_top50_audio_features'
    save_df(title, top50_audio_features_df)

    # group artist info
    boy_group_info_df = create_df_from_catalog('kpop_boy_group_artist_info')
    girl_group_info_df = create_df_from_catalog('kpop_girl_group_artist_info')

    boy_group_info_with_gender_df = boy_group_info_df.withColumn('gender', lit(0))
    girl_group_info_with_gender_df = girl_group_info_df.withColumn('gender', lit(1))

    group_info_df = boy_group_info_with_gender_df.unionAll(girl_group_info_with_gender_df)
    title = 'preprocessed_group_artist_info'
    save_df(title, group_info_df)

    # group track info
    boy_group_track_info_df = create_df_from_catalog('kpop_boy_group_track_info')
    girl_group_track_info_df = create_df_from_catalog('kpop_girl_group_track_info')

    group_track_info_df = boy_group_track_info_df.unionAll(girl_group_track_info_df)
    title = 'preprocessed_group_track_info'
    save_df(title, group_track_info_df)

    spark.stop()
