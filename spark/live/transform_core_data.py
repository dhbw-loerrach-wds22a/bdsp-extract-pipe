from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, sha2, concat_ws, coalesce, lit
import datetime


def initialize_spark_session():
    return SparkSession.builder \
        .appName("DatenTransformation") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def read_data_from_minio(spark):
    today = datetime.date.today().strftime('%Y-%m-%d')
    file_path = f"s3a://datacache/extracted_data_{today}.csv"
    return spark.read.csv(file_path, header=True, inferSchema=True)


def transform_data(df):
    split_col = split(coalesce(col("category_code"), lit("unknown.unknown.unknown")), "\\.")
    df = df.withColumn("main_category", split_col.getItem(0))
    df = df.withColumn("sub_category", split_col.getItem(1))
    df = df.withColumn("subsub_category", split_col.getItem(2))
    return df.select("event_time", "event_type", "product_id", "category_id", "main_category", "sub_category", "subsub_category", "brand", "price", "user_id", "user_session")


def write_data_to_minio(df):
    today = datetime.date.today().strftime('%Y-%m-%d')
    file_path = f"s3a://datacache/core_data_{today}.csv"
    df.write.mode("overwrite").option("header", "true").csv(file_path)


def main():
    spark = initialize_spark_session()
    df = read_data_from_minio(spark)
    transformed_df = transform_data(df)
    write_data_to_minio(transformed_df)
    spark.stop()


if __name__ == "__main__":
    main()
