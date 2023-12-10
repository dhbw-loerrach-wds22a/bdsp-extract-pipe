from pyspark.sql import SparkSession

from pyspark.sql.functions import col, max
# Initialize Spark session

spark = SparkSession.builder \
    .appName("Temperature Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()



# Read the data

df = spark.read.csv("s3a://pythonminio/city_temperature.csv", header=True, inferSchema=True)

print(df.head())

#