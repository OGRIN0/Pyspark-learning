from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("csv") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

df = spark.read.csv("data/AAPL.csv")

df.show()

