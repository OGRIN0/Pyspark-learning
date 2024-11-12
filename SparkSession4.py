#setting env
import os
os.environ['SPARK_HOME'] = "/Users/amarnath/Documents/amarss"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'


from pyspark.sql import SparkSession


#creating a array named numbers and assigning it for a rdd
numbers = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(numbers)

a =rdd.collect()
print(a)
#collect method retrives

data = [("Luke", 25), ("Cat", 30), ("Dog", 35), ("Awake", 40)]
rdd = spark.sparkContext.parallelize(data)

print("All elements of the rdd: ", rdd.collect())

#count() method returns the no.of elements
count = rdd.count()
print("The total number of elements in rdd: ", count)


#retrives the first element of rdd
first_element = rdd.first()
print( first_element)

#prints out all the elemnts
rdd.foreach(lambda x: print(x))