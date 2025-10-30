from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta
import subprocess

def run_etl_process():
    command="source ~/data_engineering/.venvs/airflow_env/bin/activate && python3 ~/data_engineering/git/jsr_etl/etl/py_transfms/user_feed_to_kafka.py"
    subprocess.run(command,shell=True,check=True)

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2025, 1, 1),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

with DAG('Send_userdata_to_kafka',
            default_args=default_args,
            schedule_interval=None,
            catchup=False) as dag:
    
    send_user_data_to_kafka = PythonOperator(
        task_id='send_user_data_to_kafka_task',
        python_callable=run_etl_process
       
    )

    success_message = BashOperator(
        task_id='success_message_task',
        bash_command='echo "User data has been successfully sent to Kafka topic."',
        trigger_rule='all_success'
    )
    failure_message = BashOperator(
        task_id='failure_message_task',
        bash_command='echo "Failed to send user data to Kafka topic."',
        trigger_rule='one_failed'
    )   

    send_user_data_to_kafka >> [success_message , failure_message]
    
