from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

spark_master = "spark://spark:7077"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 13),
}

dag = DAG('historic_data_pipeline', default_args=default_args, schedule_interval=None)

h_extract_data = SparkSubmitOperator(
    task_id='h_extract_data',
    application='./spark/historic/h_extract_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_transform_core_data = SparkSubmitOperator(
    task_id='h_transform_core_data',
    application='./spark/historic/h_transform_core_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_transform_error_values = SparkSubmitOperator(
    task_id='h_transform_error_values',
    application='./spark/historic/h_transform_error_values.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_transform_free_products = SparkSubmitOperator(
    task_id='h_transform_free_products',
    application='./spark/historic/h_transform_free_products.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_transform_revenue_data = SparkSubmitOperator(
    task_id='h_transform_revenue_data',
    application='./spark/historic/h_transform_revenue_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_load_data = SparkSubmitOperator(
    task_id='h_load_data',
    application='./spark/historic/h_load_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_transform_ml_data = SparkSubmitOperator(
    task_id='h_transform_ml_data',
    application='./spark/historic/h_transform_ml_data.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_delete_cache = SparkSubmitOperator(
    task_id='h_delete_cache',
    application='./spark/historic/h_delete_cache.py',  # Update this path
    conn_id='spark',  # Make sure this connection is configured in Airflow
    executor_memory='16g',
    total_executor_cores='1',
    name='airflow_spark_job',
    conf={"spark.master": spark_master},
    dag=dag,
)

h_extract_data >> h_transform_core_data >> h_load_data
h_extract_data >> h_transform_core_data >> h_transform_revenue_data >> h_load_data
h_extract_data >> h_transform_core_data >> h_transform_free_products >> h_load_data
h_extract_data >> h_transform_core_data >> h_transform_error_values >> h_load_data
h_load_data >> h_transform_ml_data >> h_delete_cache