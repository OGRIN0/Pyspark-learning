from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Hello").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()


numbers = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(numbers)
rdd.collect()
data = [("Wilson",10),("Luke",30),("Jhon",25)]
rdd = spark.sparkContext.parallelize(data)

print("All elemnts of the rdd", rdd.collect())

#count action
print("Total no.of elements:",rdd.count())
#first action return first element
print("First element:",rdd.first())
#Take action retribes specific elements
taken_elemnts = rdd.take(3)
print("first three elements:",taken_elemnts)

#For each action: Prrinting all elemnts:
rdd.foreach(lambda x: print(x))



