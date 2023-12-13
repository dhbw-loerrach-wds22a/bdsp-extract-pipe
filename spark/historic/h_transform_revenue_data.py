from pyspark.sql import SparkSession
import datetime

def initialize_spark_session():
    return SparkSession.builder \
        .appName("Extract Revenue Data") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def read_historical_data(spark):
    file_path = f"s3a://datacache/h_core_data.csv"
    return spark.read.csv(file_path, header=True, inferSchema=True)


def select_relevant_columns(df):
    return df.select("event_time", "product_id", "main_category", "sub_category", "brand", "price")


def write_historical_data(df):
    file_path = f"s3a://datacache/h_revenue_data.csv"
    df.write.mode("overwrite").option("header", "true").csv(file_path)


def main():
    spark = initialize_spark_session()

    df = read_historical_data(spark)
    df_relevant = select_relevant_columns(df)
    write_historical_data(df_relevant)

    spark.stop()

if __name__ == "__main__":
    main()