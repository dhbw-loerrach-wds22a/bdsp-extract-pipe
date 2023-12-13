from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan
import datetime


def initialize_spark_session():
    return SparkSession.builder \
        .appName("ML Data Preparation") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def read_new_data(spark):
    today = datetime.date.today().strftime('%Y-%m-%d')
    file_path = f"s3a://datawarehouse/core_data_{today}.csv"
    return spark.read.csv(file_path, header=True, inferSchema=True)


def filter_valid_data(df):
    return df.filter(~(isnan(col("category_code")) | isnan(col("product_id"))))


def append_to_existing_ml_data(df):
    existing_file_path = "s3a://datacache/ml_data.csv"
    df.write.mode("append").option("header", "true").csv(existing_file_path)


def main():
    spark = initialize_spark_session()
    df_new = read_new_data(spark)
    df_valid = filter_valid_data(df_new)
    append_to_existing_ml_data(df_valid)
    spark.stop()

if __name__ == "__main__":
    main()