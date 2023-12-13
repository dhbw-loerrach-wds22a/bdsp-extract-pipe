from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

spark_master = "spark://spark:7077"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 13),
}

dag = DAG('spark_job_dag', default_args=default_args, schedule_interval=None)

extract_data = SparkSubmitOperator(
    task_id='extract_data',
    application='./spark/extract_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='2g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

transform_core_data = SparkSubmitOperator(
    task_id='transform_core_data',
    application='./spark/transform_core_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='2g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

transform_error_values = SparkSubmitOperator(
    task_id='transform_error_values',
    application='./spark/transform_error_values.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='2g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

transform_free_products = SparkSubmitOperator(
    task_id='transform_free_products',
    application='./spark/transform_free_products.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='2g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

transform_revenue_data = SparkSubmitOperator(
    task_id='transform_revenue_data',
    application='./spark/transform_revenue_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='2g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

load_data = SparkSubmitOperator(
    task_id='load_data',
    application='./spark/load_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='2g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

extract_data >> transform_core_data >> load_data
extract_data >> transform_core_data >> transform_revenue_data >> load_data
extract_data >> transform_free_products >> load_data
extract_data >> transform_error_values >> load_data
