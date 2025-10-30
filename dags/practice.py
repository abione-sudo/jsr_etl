from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def data_extraction(**context):
    print("Extracting data...")
    df=pd.DataFrame("/Users/abeee/data_engineering/data/raw_data/users.csv")
    context['ti'].xcom_push(key='extracted_count', value=len(df))
    print(df.head())
    return df

def data_transformation(**context):
    print(f"Transforming data...total rows : {context['ti'].xcom_pull(key='extracted_count')}")
    df=context['ti'].xcom_pull(task_ids='data_extraction_task')
    df_agg=df.groupby('name').count().reset_index()
    return df_agg

def data_loading(**context):
    print(f"Loading data...total rows : {len(context['ti'].xcom_pull(task_ids='data_transformation_task'))}")
    df=context['ti'].xcom_pull(task_ids='data_transformation_task')
    df.to_csv("/Users/abeee/data_engineering/data/clean_data/users_agg.csv", index=False)
    print("Data loaded successfully.")

default_args = {
    'owner' : 'airflow',
    'retries' : 1 ,
    'retry_delay' : timedelta(minutes=1) }

with DAG(
    dag_id='etl_practice_dag',
    default_args=default_args,
    description='A simple ETL DAG for practice',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    data_extraction_task = PythonOperator(
        task_id='data_extraction_task',
        python_callable=data_extraction,
        provide_context=True
    )

    data_transformation_task = PythonOperator(
        task_id='data_transformation_task',
        python_callable=data_transformation,
        provide_context=True,
        trigger_rule='all_success'
    )
    data_loading_task = PythonOperator(
        task_id='data_loading_task',
        python_callable=data_loading,
        provide_context=True,
        trigger_rule='all_success'
    )
    data_extraction_task >> data_transformation_task >> data_loading_task
