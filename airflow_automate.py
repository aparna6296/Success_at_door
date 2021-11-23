from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'depends_on_past': False,
    'email': False,
    'email_on_failure': False,
    'email_on_retries': False,
    'retires': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'GA_Insight',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

task = BashOperator(
    task_id="run",
    bash_command="sh <absolute_path>/run.sh <source_location> <target_location>",
    dag=dag
)
