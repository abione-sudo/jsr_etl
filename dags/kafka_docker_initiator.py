from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args= {
    'owner' : 'airflow',
    'start_date' : datetime(2025, 6, 1),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

def success_callback(context):
    print("🎉 Task succeeded:", context['task_instance'].task_id)

def failure_callback(context):
    print("💥 Task failed:", context['task_instance'].task_id)

with DAG('kafka_docker_initiator',
            default_args=default_args,
            schedule_interval=None,
            catchup=False) as dag:
         
    docker_task = BashOperator(
        task_id='start_kafka',
        bash_command='cd /Users/abeee/data_engineering/docker/confluent-kafka && docker-compose up -d'

    )

    success_task = BashOperator(
        task_id='kafka_started',
        bash_command='echo "Kafka Docker containers started successfully."'
    )

    failure_task = BashOperator(
        task_id='kafka_start_failed',
        bash_command='echo "Failed to start Kafka Docker containers."'  
    )

    docker_task >> [success_task, failure_task] 