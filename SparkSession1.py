#implementating SparkSession
import os
os.environ['SPARK_HOME'] = "/Users/amarnath/Documents/amarss"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark-Get-Started") \
    .getOrCreate()

