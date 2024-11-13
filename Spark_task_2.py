from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import*
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("hello-world") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

data =  [
    (1, '2024-11-01', 100.0), (1, '2024-11-02', 150.0), (1, '2024-11-03', 200.0),
    (1, '2024-11-05', 130.0), (1, '2024-11-07', 80.0), (1, '2024-11-09', 70.0),
    (1, '2024-11-10', 60.0), (1, '2024-11-11', 90.0), (1, '2024-11-13', 110.0),
    (2, '2024-11-01', 50.0),  (2, '2024-11-02', 60.0),  (2, '2024-11-04', 40.0),
    (2, '2024-11-06', 90.0),  (2, '2024-11-08', 100.0), (2, '2024-11-10', 110.0),
    (2, '2024-11-11', 70.0),  (2, '2024-11-13', 130.0), (3, '2024-11-02', 300.0),
    (3, '2024-11-04', 200.0), (3, '2024-11-06', 400.0), (3, '2024-11-08', 150.0),
    (3, '2024-11-10', 250.0), (3, '2024-11-11', 200.0), (3, '2024-11-13', 100.0)
]
columns = ["customer_id", "transaction_date", "amount"]
transactions = spark.createDataFrame(data, columns)
transactions = transactions.withColumn("transaction_date", col('transaction_date').cast('date'))

window = Window.partitionBy('customer_id').orderBy('transaction_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)

avg = Window.partitionBy('customer_id').orderBy('transaction_date').rowsBetween(-6, 0)
cum_transaction= transactions.withColumn('cumulative_amount', sum('amount').over(window))
trans_avg = cum_transaction.withColumn('rolling_avg_amount', avg('amount').over(avg))

trans_avg.show()



