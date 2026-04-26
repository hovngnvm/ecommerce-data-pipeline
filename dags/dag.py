import sys
from pathlib import Path
import requests
from datetime import datetime, timedelta

# Ensure project root and scripts directory are in sys.path
DAG_DIR = Path(__file__).resolve().parent
PROJECT_DIR = DAG_DIR.parent
SCRIPTS_DIR = PROJECT_DIR / "scripts"
DBT_DIR = PROJECT_DIR / "dbt"

if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Variable
from airflow.providers.standard.operators.bash import BashOperator

from scripts.utils.spark import SPARK_PACKAGES
from scripts.utils.logger import get_logger
from scripts.upload_to_bronze import run_upload
from scripts.silver_to_olap import run_silver_to_olap

logger = get_logger(__name__)

def send_telegram_alert(context: dict) -> None:
    """Sends a failure alert via Telegram bot."""
    bot_token = Variable.get('telegram_bot_token', default_var='dummy_token')
    chat_id = Variable.get('telegram_chat_id', default_var='dummy_chat_id')

    if bot_token == 'dummy_token' or chat_id == 'dummy_chat_id':
        return

    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    error_reason = str(exception)[:500]

    message = f"!!! AIRFLOW ALERT !!!\nFailed Task: {task_id}\nExecution Date: {execution_date}\nError: {error_reason}"
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'
    }
    try:
        res = requests.post(url, data=payload, timeout=10)
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending Telegram alert: {e}")

default_args = {
    'owner': 'd1ego23',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
    'on_failure_callback': send_telegram_alert
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

    # Spark submit task via BashOperator (external CLI process management)
    spark_packages_arg = f'--packages {SPARK_PACKAGES}'
    bronze_to_silver_task = BashOperator(
        task_id='bronze_to_silver',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 1536M {spark_packages_arg} '
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

    dbt_docs_task = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_DIR} && dbt docs generate --profiles-dir .'
    )

    # TaskFlow API Dependencies with Quality Gate
    t_bronze = ingest_to_bronze_task()
    t_olap = silver_to_olap_task()

    t_bronze >> bronze_to_silver_task >> t_olap >> dbt_build_task >> dbt_docs_task
