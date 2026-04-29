from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator

from scripts.upload_to_bronze import run_upload
from scripts.silver_to_olap import run_silver_to_olap

PROJECT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_DIR / "scripts"
DBT_DIR = PROJECT_DIR / "dbt"

default_args = {
    'owner': 'd1ego23',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'ecommerce_medallion_pipeline',
    default_args=default_args,
    description="E-Commerce Medallion Pipeline",
    schedule=timedelta(days=1),
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['ecommerce', 'spark', 'medallion', 'neon', 'minio', 'taskflow'],
) as dag:

    @task(task_id='ingest_to_bronze')
    def ingest_to_bronze_task(ds: str | None = None) -> None:
        """Python TaskFlow operator for uploading staging parquet to Bronze MinIO lake"""
        run_upload(ds)

    bronze_to_silver_task = BashOperator(
        task_id='bronze_to_silver',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 1536M '
                     f'{SCRIPTS_DIR / "bronze_to_silver.py"} ' + '{{ ds }}'
    )

    @task(task_id='silver_to_olap')
    def silver_to_olap_task(ds: str | None = None) -> None:
        """Python TaskFlow operator for DuckDB OLAP ingestion"""
        run_silver_to_olap(ds)

    dbt_build_task = BashOperator(
        task_id='dbt_build_quality_gate',
        bash_command=f'cd {DBT_DIR} && dbt build --fail-fast --store-failures --profiles-dir .'
    )

    # TaskFlow API Dependencies with Quality Gate
    t_bronze = ingest_to_bronze_task()
    t_olap = silver_to_olap_task()

    t_bronze >> bronze_to_silver_task >> t_olap >> dbt_build_task

