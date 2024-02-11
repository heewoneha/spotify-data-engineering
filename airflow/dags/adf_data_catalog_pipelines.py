from airflow.models import DAG, BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from datetime import datetime, timedelta


with DAG(
    dag_id="azure_data_factory_run_data_catalog_pipelines",
    start_date=datetime(2024, 2, 11),
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

    run_pipeline1: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline1",
        pipeline_name="scraper_data_catalog",
    )

    run_pipeline2: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline2",
        pipeline_name="preprocessing_data_catalog",
    )

    begin >> run_pipeline1 >> run_pipeline2 >> end
