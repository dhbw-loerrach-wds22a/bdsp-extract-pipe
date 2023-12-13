from pyspark.sql import SparkSession
import mysql.connector
from mysql.connector import Error
import datetime

from minio_bucket import *

def initialize_spark_session():
    return SparkSession.builder \
        .appName("Extract Data") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def read_data_from_mysql():
    try:
        connection = mysql.connector.connect(
            host='mysql',          
            database='customer_data',  
            user='root',               
            password='mypassword'      
        )
        cursor = connection.cursor()
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        query = f"SELECT DISTINCT * FROM events WHERE event_time = '{yesterday}'"
        cursor.execute(query)

        result = cursor.fetchall()
        columns = cursor.column_names
        connection.close()
        return columns, result
    except Error as e:
        print(f"Fehler beim Lesen der Daten aus MySQL: {e}")
        return None, None


def write_data_to_minio(spark, data, columns):
    df = spark.createDataFrame(data, schema=columns)
    today = datetime.date.today().strftime('%Y-%m-%d')
    df.write.mode("overwrite").option("header", "true").csv(f"s3a://datacache/extracted_data_{today}.csv")
    print("Daten erfolgreich in MinIO hochgeladen.")


def main():
    columns, data = read_data_from_mysql()
    if data:
        spark = initialize_spark_session()
        write_data_to_minio(spark, data, columns)
        spark.stop()


if __name__ == "__main__":
    main()
