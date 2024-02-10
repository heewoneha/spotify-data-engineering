from airflow.models import DAG, BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from datetime import datetime, timedelta


with DAG(
    dag_id="azure_data_factory_run_scraper_data_catalog_pipeline",
    start_date=datetime(2024, 2, 10),
    schedule="30 2 * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "azure_data_factory_conn_id": "adf_conn",
        "factory_name": "cosmicETL",
        "resource_group_name": "airflow",
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    run_pipeline: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline",
        pipeline_name="scraper_data_catalog",
    )

    begin >> run_pipeline >> end
