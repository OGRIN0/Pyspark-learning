from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("zello").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()


numbers = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(numbers)
rdd.collect()
data = [("Wilson",10),("Luke",30),("Jhon",25)]
rdd = spark.sparkContext.parallelize(data)


#map transformation modifies specified or all the elemnts as we specify
mapped_rdd = rdd.map(lambda x: [x[0].upper(), x[1]])
r = mapped_rdd.collect()
print(r)

#filter transformaation filter out the data based on the condition give
filtered_rdd =  rdd.filter(lambda x: x[1] > 30)
v = filtered_rdd.collect()
print(v)

#saveAsextFile transformation save files as txt file
rdd_txt = rdd.saveAsTextFile("output.txt")

rdd_text = spark.sparkContext.textFile("output.txt")
p = rdd_text.collect()
