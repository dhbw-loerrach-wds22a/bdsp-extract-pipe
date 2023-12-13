from pyspark.sql import SparkSession
import datetime


def initialize_spark_session():
    return SparkSession.builder \
        .appName("Delete Old Data Files") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def delete_all_files(spark):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    datacache_path = 's3a://datacache/'

    try:
        list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(datacache_path))
        for status in list_status:
            file_path = status.getPath().toString()
            print(f"Lösche Datei: {file_path}")
            fs.delete(spark._jvm.org.apache.hadoop.fs.Path(file_path), False)
    except Exception as e:
        print(f"Fehler beim Zugriff auf das Verzeichnis: {e}")


def main():
    spark = initialize_spark_session()
    delete_all_files(spark)
    spark.stop()

if __name__ == "__main__":
    main()
