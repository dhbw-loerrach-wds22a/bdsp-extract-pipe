from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


def initialize_spark_session():
    return SparkSession.builder \
        .appName("DatenVerschiebung") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def move_data(spark, source_path, destination_path):
    try:
        df = spark.read.csv(source_path, header=True, inferSchema=True)
        df.write.mode("overwrite").option("header", "true").csv(destination_path)
        print(f"Daten von {source_path} nach {destination_path} verschoben.")
    except AnalysisException as e:
        print(f"Datei {source_path} nicht gefunden, Überspringen des Verschiebens.")


def main():
    spark = initialize_spark_session()

    files_to_move = ["core_data.csv", "revenue_data.csv", "free_product_data.csv", "error_values.csv"]
    source_bucket = "s3a://datacache/"
    destination_bucket = "s3a://datawarehouse/"

    for file_name in files_to_move:
        move_data(spark, source_bucket + file_name, destination_bucket + file_name)

    spark.stop()


if __name__ == "__main__":
    main()
