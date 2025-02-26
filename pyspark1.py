from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder\
    .appName("app")\
    .master("local[*]")\
    .getOrCreate()

def load_stock_data():
    df = spark.read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv("file:/Users/amarnath/pyspark/datasets/data.csv")

    # df.show(5, truncate=False)  
    # df.printSchema()  
    


    return df.select(
        col("InvoiceDate").alias("date"),
        col("InvoiceNo").cast(DoubleType()).alias("invoiceno"),
        col("StockCode").cast(StringType()).alias("stockcode"),
        col("Quantity").cast(IntegerType()).alias("quantity"),
        col("UnitPrice").cast(DoubleType()).alias("unitprice"),
        col("CustomerID").cast(IntegerType()).alias("customerid"),
        col("Country").cast(StringType()).alias("country")
    )

coke = load_stock_data()
coke = coke.withColumn("date", col("date").cast(StringType()))  

date_plus_2 = date_add(col('date'), 2)  
coke_transformed = coke.withColumn("transformedDate", concat(col("date").cast(StringType()), lit("app")))

# coke_transformed.select("date", "transformedDate").show(truncate=False)

# spark.sql("select date, cast(date_add(date, 2) as string) as transformedDate from df") \
#     .show()

# coke.sort("date").show()
# coke.sort(coke['date'].desc()).show()
# coke.sort(date_add(coke['date'], 2).desc()).show()

# coke.groupby("date").max("unitprice").show()
# coke.groupby(year(coke['date'])).agg(max('unitprice'), avg('unitprice')).show()

# window  = Window.partitionBy(year(coke['date'])).orderBy(coke['unitprice'].desc(), coke['date'])

# coke.withColumn("rank", row_number().over(window)).show()

window = Window.partitionBy().orderBy(coke['date'])
coke.withColumn("customerid", lag(coke['unitprice'],1,0.0).over(window))\
    .show()
