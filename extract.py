# from pyspark.sql import SparkSession
#
# from pyspark.sql.functions import col, max
# # Initialize Spark session
#
# spark = SparkSession.builder \
#     .appName("Temperature Analysis") \
#     .master("spark://spark-master:7077") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", True) \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
#     .getOrCreate()
#
#
#
# # Read the data
#
# df = spark.read.csv("s3a://pythonminio/city_temperature.csv", header=True, inferSchema=True)
#
# print(df.head())
#
#
#
# #

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySQL Data Fetch") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

import mysql.connector
from mysql.connector import Error

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='mysql',          # replace with your host, e.g., 'localhost'
            database='customer_data',        # replace with your database name
            user='root',      # replace with your database username
            password='mypassword'   # replace with your database password
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print("Error while connecting to MySQL", e)
connect_to_database()
