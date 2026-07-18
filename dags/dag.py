import os
import requests
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# Airflow paths mapping inside the container
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(DAG_DIR)
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")
DBT_DIR = os.path.join(PROJECT_DIR, "dbt")

def send_telegram_alert(context):
    """
    Sends a failure alert via Telegram bot.
    """
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
    description="E-Commerce Medallion Pipeline",
    schedule=timedelta(days=1), 
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['ecommerce', 'spark', 'medallion', 'neon', 'minio'],
) as dag:
    
    ingest_to_bronze_task = BashOperator(
        task_id='ingest_to_bronze',
        bash_command=f'python3 {os.path.join(SCRIPTS_DIR, "upload_to_bronze.py")} ' + '{{ ds }}'
    )

    # Maven package dependencies for Spark to load Delta Lake, Postgres, and MinIO S3 support
    spark_packages = '--packages org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.6.0'

    bronze_to_silver_task = BashOperator(
        task_id='bronze_to_silver',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 1536M {spark_packages} '
                     f'{os.path.join(SCRIPTS_DIR, "bronze_to_silver.py")} ' + '{{ ds }}'
    )

    silver_to_rdbms_task = BashOperator(
        task_id='silver_to_rdbms',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 1G {spark_packages} '
                     f'{os.path.join(SCRIPTS_DIR, "silver_to_rdbms.py")} ' + '{{ ds }}'
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run_star_schema',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir .',
        dag=dag
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=f'cd {DBT_DIR} && dbt test --profiles-dir .',
        dag=dag
    )

    dbt_docs_task = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_DIR} && dbt docs generate --profiles-dir .',
        dag=dag
    )
    
    # Task dependencies definition
    ingest_to_bronze_task >> bronze_to_silver_task >> silver_to_rdbms_task >> dbt_run_task >> dbt_test_task >> dbt_docs_task
