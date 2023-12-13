from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def initialize_spark_session():
    return SparkSession.builder \
        .appName("Extract Free Products") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def read_data(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)


def filter_free_products(df):
    return df.filter(col("price") == 0)


def select_relevant_columns(df):
    return df.select("product_id", "brand", "price")


def write_data(df, file_path):
    df.write.mode("overwrite").option("header", "true").csv(file_path)


def main():
    spark = initialize_spark_session()
    df = read_data(spark, "s3a://datacache/extracted_data.csv")
    
    df_free_products = filter_free_products(df)
    df_relevant = select_relevant_columns(df_free_products)

    # Ausgabe der Liste der kostenlosen Produkte
    free_product_list = df_relevant.collect()
    print("Liste der kostenlosen Produkte:")
    for product in free_product_list:
        print("Product ID:", product["product_id"], "- Brand:", product["brand"])

    write_data(df_relevant, "s3a://datacache/free_product_data.csv")
    spark.stop()

if __name__ == "__main__":
    main()