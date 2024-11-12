import os
os.environ['SPARK_HOME'] = "/Users/amarnath/Documents"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark-Get-Started") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()