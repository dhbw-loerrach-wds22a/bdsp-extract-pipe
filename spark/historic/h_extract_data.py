from pyspark.sql import SparkSession
import mysql.connector
from mysql.connector import Error
import datetime

from minio_bucket import *

def initialize_spark_session():
    return SparkSession.builder \
        .appName("Extract Historical Data") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def read_historical_data_from_mysql(batch_size, offset=0):
    try:
        connection = mysql.connector.connect(
            host='mysql',
            database='customer_data',
            user='root',
            password='mypassword'
        )
        cursor = connection.cursor()
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        query = f"SELECT DISTINCT * FROM events WHERE event_time <= '{yesterday}' LIMIT {batch_size} OFFSET {offset}"
        cursor.execute(query)

        result = cursor.fetchall()
        columns = cursor.column_names
        connection.close()
        return columns, result
    except Error as e:
        print(f"Fehler beim Lesen der historischen Daten aus MySQL: {e}")
        return None, None

def write_historical_data_to_minio(spark, data, columns, mode):
    df = spark.createDataFrame(data, schema=columns)
    df.write.mode(mode).option("header", "true").csv(f"s3a://datacache/h_extracted_data.csv")
    print("Batch of historical data successfully uploaded to MinIO.")

def main():
    batch_size = 100000  # Define your batch size
    offset = 0
    spark = initialize_spark_session()
    while True:
        columns, data = read_historical_data_from_mysql(batch_size, offset)
        if not data:
            break
        write_mode = "append" if offset > 0 else "overwrite"
        write_historical_data_to_minio(spark, data, columns, write_mode)
        offset += batch_size
    spark.stop()

if __name__ == "__main__":
    main()