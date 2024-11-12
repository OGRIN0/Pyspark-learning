import os
os.environ['SPARK_HOME'] = "/Users/amarnath/Documents/amarss"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext(appName="MySparkApplication")

sc
#instantiating the session
sc.stop()
#ending the session

spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .getOrCreate()
#this is the creation of spark session

sc = spark.sparkContext
#Access sparkcontext via sparksession

sc 
#Again instantiate the session
sc.stop()
#End the session
