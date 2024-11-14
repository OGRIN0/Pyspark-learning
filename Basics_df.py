from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("DF-Demo").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

#doing top 5 most freq word using rdd
rdd = spark.sparkContext.textFile("./data/names_ages.txt")
result_rdd = rdd.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word,1))\
    .reduceByKey(lambda word: (word,1))\
    .sortBy(lambda x: x[1], ascending=False)

a =result_rdd.take(5)
print(a)

#doing thesame operation with dataframe
df = spark.sparkContext.textFile("./data/names_ages_txt")

#slecetExpr method throwing out error - not in the program but in the mathod doesn't seems to exist within the SparkSession
result_df = df.selectExpr("explode(splitvalue(value, ' ')) as word")\
    .groupBy("word").count().orderBy(desc("count"))
result_df.take(5)
spark.stop()





