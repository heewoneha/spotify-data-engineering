from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time


SCHEMA = 'raw'


def short_delay():
    time.sleep(10)


default_args = {
    'retries': 1,
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgres_making_raw_schema_tables',
    default_args=default_args,
    description='A DAG to execute PostgreSQL query',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 12),
    catchup=False,
    template_searchpath='/opt/airflow/dags/sqls/postgres',
)

begin = DummyOperator(task_id="begin")
end = DummyOperator(task_id="end")

raw_table_list = [
    'daily_group_artist_spotify_popularity',
    'daily_group_track_spotify_popularity',
    'group_track_information',
    'daily_top50_track',
    'top50_audio_feature',
    'top50_track_information'
]

dimension_drop_tasks = []
dimension_create_tasks = []

def process_raw_table_queries(table_list):
    for table_name in table_list:
        drop_query = f'DROP TABLE IF EXISTS {SCHEMA}.{table_name}'
        drop_task =  PostgresOperator(
            task_id=f'run_postgres_drop_{table_name}',
            sql=drop_query,
            postgres_conn_id='postgres_conn',
            autocommit=True,
            dag=dag,
        )

        delay_task = PythonOperator(
            task_id=f'short_delay_before_create_{table_name}',
            python_callable=short_delay,
            dag=dag,
        )

        create_task = PostgresOperator(
            task_id=f'run_postgres_create_{table_name}',
            sql=f'create_{table_name}.sql',
            postgres_conn_id='postgres_conn',
            autocommit=True,
            dag=dag,
        )

        drop_task >> delay_task >> create_task
        dimension_drop_tasks.append(drop_task)
        dimension_create_tasks.append(create_task)


process_raw_table_queries(raw_table_list)


begin >> dimension_drop_tasks
dimension_create_tasks >> end
