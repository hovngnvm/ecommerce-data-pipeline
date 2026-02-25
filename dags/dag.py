import os
import sys
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Variable
from airflow.providers.standard.operators.bash import BashOperator

# Ensure scripts directory is in sys.path
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(DAG_DIR)
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")
DBT_DIR = os.path.join(PROJECT_DIR, "dbt")

if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

def send_telegram_alert(context):
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
        requests.post(url, data=payload)
    except Exception as e:
        print(f"Error sending Telegram alert: {e}")

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
    description="E-Commerce Medallion Pipeline (Refactored with TaskFlow API)",
    schedule=timedelta(days=1), 
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['ecommerce', 'spark', 'medallion', 'neon', 'minio', 'taskflow'],
) as dag:

    @task(task_id='ingest_to_bronze')
    def ingest_to_bronze_task(ds=None):
        """Python TaskFlow operator for uploading staging parquet to Bronze MinIO lake"""
        import upload_to_bronze
        sys.argv = ['upload_to_bronze.py', ds]
        upload_to_bronze.main()

    # Spark submit task via BashOperator (external CLI process management)
    spark_packages = '--packages org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.6.0'
    bronze_to_silver_task = BashOperator(
        task_id='bronze_to_silver',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 1536M {spark_packages} '
                     f'{os.path.join(SCRIPTS_DIR, "bronze_to_silver.py")} ' + '{{ ds }}'
    )

    @task(task_id='silver_to_olap')
    def silver_to_olap_task(ds=None):
        """Python TaskFlow operator for DuckDB OLAP ingestion"""
        import silver_to_olap
        sys.argv = ['silver_to_olap.py', ds]
        silver_to_olap.main()

    dbt_run_task = BashOperator(
        task_id='dbt_run_star_schema',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir .'
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=f'cd {DBT_DIR} && dbt test --profiles-dir .'
    )

    dbt_docs_task = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_DIR} && dbt docs generate --profiles-dir .'
    )
    
    # TaskFlow API Dependencies
    t_bronze = ingest_to_bronze_task()
    t_olap = silver_to_olap_task()
    
    t_bronze >> bronze_to_silver_task >> t_olap >> dbt_run_task >> dbt_test_task >> dbt_docs_task
