import os
os.environ['SPARK_HOME'] = "/Users/amarnath/Documents/amarss"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'




from pyspark.sql import SparkSession

# Creating  a SparkSession
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


spark
#instantiating the spark session


spark.stop()
#ending of session
