from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

spark_master = "spark://spark:7077"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 13),
}

dag = DAG('spark_job_dag', default_args=default_args, schedule_interval=None)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='./spark/extract_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='2g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master":spark_master},
    dag=dag,
)

spark_submit_task
