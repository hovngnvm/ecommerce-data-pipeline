import requests, os
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# /dags/ecommerce_medallion_dag.py
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(DAG_DIR)

SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")

BRONZE_PATH = os.path.join(PROJECT_DIR, "data", "bronze")
SILVER_PATH = os.path.join(PROJECT_DIR, "data", "silver")

def send_telegram_alert(context):
    # Get token and chat_id from Airflow Variables
    bot_token = Variable.get('telegram_bot_token')
    chat_id = Variable.get('telegram_chat_id')
    
    # Get failed task info from Airflow context
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    
    # Get error reason (Exception) and truncate to first 500 characters to avoid overly long messages
    exception = context.get('exception')
    error_reason = str(exception)[:500]
    
    message = f"🚨 AIRFLOW ALERT 🚨\nFailed Task: {task_id}\nExecution Date: {execution_date}\nError: {error_reason}"
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'
    }
    requests.post(url, data=payload)

# default_args for the DAG, including retry logic and Telegram alert on failure
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
    description='End-to-end Data Engineering Pipeline (Bronze -> Silver -> Gold -> RDBMS)',
    schedule=timedelta(days=1), 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ecommerce', 'spark', 'medallion'],
    
) as dag:
    
    ingest_api_task = BashOperator(
        task_id='ingest_api_to_bronze',
        bash_command=f'python3 {os.path.join(SCRIPTS_DIR,"python" ,"fetch_exchange_rates.py")} {os.path.join(BRONZE_PATH, "exchange_rates", "rates_2019_10.json")}'
    )

    events_bronze_to_silver_task = BashOperator(
        task_id='events_bronze_to_silver',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 4G {os.path.join(SCRIPTS_DIR, "spark", "bronze_to_silver_events.py")} {os.path.join(BRONZE_PATH, "ecommerce_events")} {os.path.join(SILVER_PATH, "ecommerce_events") } {os.path.join(PROJECT_DIR, "data", "quarantine")}'
    )

    api_bronze_to_silver_task = BashOperator(
        task_id='api_bronze_to_silver',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 2G {os.path.join(SCRIPTS_DIR, "spark", "bronze_to_silver_api.py")} {os.path.join(BRONZE_PATH, "exchange_rates", "rates_2019_10.json")} {os.path.join(SILVER_PATH, "exchange_rates")}'
    )

    start_postgres_task = BashOperator(
        task_id='start_postgres_container',
        bash_command=f'cd {PROJECT_DIR} && docker-compose up -d'
    )

    silver_to_rdbms_task = BashOperator(
        task_id='silver_to_rdbms',
        bash_command=f'spark-submit --master "local[*]" --driver-memory 4G --packages org.postgresql:postgresql:42.6.0 {os.path.join(SCRIPTS_DIR, "spark", "silver_to_rdbms.py")} {os.path.join(SILVER_PATH, "ecommerce_events")} {os.path.join(SILVER_PATH, "exchange_rates")}'
    )

    DBT_DIR = os.path.join(PROJECT_DIR, "ecommerce_dbt")

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
    
# Define task dependencies to create the DAG flow
ingest_api_task >> api_bronze_to_silver_task
events_bronze_to_silver_task
[events_bronze_to_silver_task, api_bronze_to_silver_task] >> start_postgres_task >> silver_to_rdbms_task >> dbt_run_task >> dbt_test_task