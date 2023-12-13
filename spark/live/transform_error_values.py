from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col
import datetime

from pyspark.sql.types import DoubleType, FloatType


def initialize_spark_session():
    return SparkSession.builder \
        .appName("Extract Error Values") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def read_data(spark):
    today = datetime.date.today().strftime('%Y-%m-%d')
    file_path = f"s3a://datacache/extracted_data_{today}.csv"
    return spark.read.csv(file_path, header=True, inferSchema=True)

def find_error_values(df):
    condition = None
    for c in df.columns:
        # Check for null values in all columns
        current_condition = col(c).isNull()
        # Additional check for isnan only if the column type is DoubleType or FloatType
        if isinstance(df.schema[c].dataType, (DoubleType, FloatType)):
            current_condition = current_condition | isnan(col(c))

        condition = current_condition if condition is None else condition | current_condition

    df_errors = df.filter(condition)
    return df_errors

def write_data(df):
    today = datetime.date.today().strftime('%Y-%m-%d')
    file_path = f"s3a://datacache/error_values_{today}.csv"
    df.write.mode("overwrite").option("header", "true").csv(file_path)

def main():
    spark = initialize_spark_session()
    df = read_data(spark)
    df_errors = find_error_values(df)
    write_data(df_errors)
    spark.stop()

if __name__ == "__main__":
    main()
