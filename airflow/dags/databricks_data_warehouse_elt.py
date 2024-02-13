from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import time


SCHEMA = 'analytics'
databricks_sql_endpoint = Variable.get("databricks_sql_endpoint")

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
    'databricks_dw_making_analytics_schema_tables',
    default_args=default_args,
    description='A DAG to execute Databricks Data Warehouse query',
    schedule="0 4 * * *",
    start_date=datetime(2024, 2, 13),
    catchup=False,
    template_searchpath='/opt/airflow/dags/sqls/databricks_warehouse',
)

begin = EmptyOperator(task_id="begin")
end = EmptyOperator(task_id="end")

analytics_table_list = [
    'unique_daily_top50_track',
    'unique_top50_chart_in_count',
    'unique_top50_ranked_artist_count',
    'unique_group_popularity_followers',
    'unique_group_track_popularity',
    'unique_top50_artist_avg_audio_feature',
    'unique_group_avg_duration_ms',
    'unique_group_collaboration_artist'
]

dimension_drop_tasks = []
dimension_create_tasks = []

create_schema_task = DatabricksSqlOperator(
    databricks_conn_id='databricks_conn',
    sql_endpoint_name=databricks_sql_endpoint,
    task_id='run_dw_create_schema',
    sql='create_schema.sql',
    dag=dag,
)

drop_unique_group_artist_task = DatabricksSqlOperator(
    databricks_conn_id='databricks_conn',
    sql_endpoint_name=databricks_sql_endpoint,
    task_id='run_dw_drop_unique_group_artist',
    sql='DROP TABLE IF EXISTS {SCHEMA}.unique_group_artist',
    dag=dag,
)

delay_unique_group_artist_task = PythonOperator(
    task_id='short_delay_before_create_unique_group_artist',
    python_callable=short_delay,
    dag=dag,
)

create_unique_group_artist_task = DatabricksSqlOperator(
    databricks_conn_id='databricks_conn',
    sql_endpoint_name=databricks_sql_endpoint,
    task_id='run_dw_create_unique_group_artist',
    sql='create_unique_group_artist.sql',
    dag=dag,
)


def process_analytics_table_queries(table_list):
    for table_name in table_list:
        drop_query = f'DROP TABLE IF EXISTS {SCHEMA}.{table_name}'
        drop_task =  DatabricksSqlOperator(
            databricks_conn_id='databricks_conn',
            sql_endpoint_name=databricks_sql_endpoint,
            task_id=f'run_dw_drop_{table_name}',
            sql=drop_query,
            dag=dag,
        )

        delay_task = PythonOperator(
            task_id=f'short_delay_before_create_{table_name}',
            python_callable=short_delay,
            dag=dag,
        )

        create_task = DatabricksSqlOperator(
            databricks_conn_id='databricks_conn',
            sql_endpoint_name=databricks_sql_endpoint,
            task_id=f'run_dw_create_{table_name}',
            sql=f'create_{table_name}.sql',
            dag=dag,
        )

        drop_task >> delay_task >> create_task
        dimension_drop_tasks.append(drop_task)
        dimension_create_tasks.append(create_task)


process_analytics_table_queries(analytics_table_list)


begin >> create_schema_task >> drop_unique_group_artist_task >> delay_unique_group_artist_task >> create_unique_group_artist_task >> dimension_drop_tasks
dimension_create_tasks >> end
