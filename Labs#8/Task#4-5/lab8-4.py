from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType


spark = SparkSession.builder.appName("lab8-4").getOrCreate()

lines = spark.read.csv("./customer-orders.csv", header=False)

customerSchema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("itemID", IntegerType(), True),
    StructField("amountSpent", FloatType(), True)])

schemaOrders = spark.read.schema(customerSchema).csv("./customer-orders.csv")

schemaOrders.createOrReplaceTempView("orders")
totalAmountSpentByCustomer = spark.sql("""
    SELECT customerID, SUM(amountSpent) AS totalAmountSpent
    FROM orders
    GROUP BY customerID
""")
totalAmountSpentByCustomer.show()

spark.stop()
