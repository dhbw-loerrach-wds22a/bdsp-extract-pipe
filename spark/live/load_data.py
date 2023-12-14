from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
import datetime


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
        df_source = spark.read.csv(source_path, header=True, inferSchema=True)

        try:
            df_destination = spark.read.csv(destination_path, header=True, inferSchema=True)
            df_combined = df_destination.union(df_source).distinct()
        except AnalysisException:
            # Falls die Datei im Zielverzeichnis nicht existiert, wird nur die Quelldatei verwendet
            df_combined = df_source

        df_combined.write.mode("append").option("header", "true").csv(destination_path)
        print(f"Daten von {source_path} nach {destination_path} verschoben.")
    except AnalysisException as e:
        print(f"Datei {source_path} nicht gefunden, Überspringen des Verschiebens.")


def main():
    spark = initialize_spark_session()

    today = datetime.date.today().strftime('%Y-%m-%d')
    files_to_move = [f"core_data_{today}.csv", f"revenue_data_{today}.csv", f"free_product_data_{today}.csv", f"error_values_{today}.csv", f"ml_data_{today}.csv"]
    source_bucket = "s3a://datacache/"
    destination_bucket = "s3a://datawarehouse/"

    for file_name in files_to_move:
        move_data(spark, source_bucket + file_name, destination_bucket + file_name.replace(f"_{today}", ""))

    spark.stop()

if __name__ == "__main__":
    main()
