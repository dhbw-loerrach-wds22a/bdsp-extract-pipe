from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
import os

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

# Spark Session initialisieren
spark = initialize_spark_session()

# Daten aus S3 einlesen
data = spark.read.csv('s3a://datacache/ml_data.csv', header=True, inferSchema=True)

# Window-Funktion definieren, um eine Zeilennummer zu jeder Zeile hinzuzufügen
windowSpec = Window.orderBy(desc("timestamp"))  # Annahme: Es gibt eine Spalte 'timestamp'
data = data.withColumn("row_num", row_number().over(windowSpec))

# Datenaufteilung: Letzte 8 Mio. Einträge für Training, letzte 2 Mio. Einträge für Testen
num_train_rows = 8000000
num_test_rows = 2000000
train_data = data.filter(data["row_num"] > num_test_rows).limit(num_train_rows)
test_data = data.filter(data["row_num"] <= num_test_rows)

# Umwandlung der Daten für ALS
train_data = train_data.withColumn("rating", train_data["event_type"].cast("integer"))
test_data = test_data.withColumn("rating", test_data["event_type"].cast("integer"))

# ALS Modell
als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="product_id", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(train_data)

# Vorhersagen auf Testdaten machen
predictions = model.transform(test_data)

# Bewertung des Modells
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)

# Ausgabe der Bewertungsergebnisse
print(f"Root Mean Squared Error: {rmse}")

# Modell speichern
model_directory = '/Users/kilianlorenz/Uni/Big_Data/Model'
model.write().overwrite().save(model_directory)

# Spark Session beenden
spark.stop()