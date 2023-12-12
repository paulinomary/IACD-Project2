from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# Create SparkSession
spark = SparkSession.builder.appName("lab8-4").getOrCreate()

# Load each line of the source data into a dataset
lines = spark.read.csv("./customer-orders.csv", header=False)

# Create a schema for the dataframe
customerSchema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("itemID", IntegerType(), True),
    StructField("amountSpent", FloatType(), True)])

# Apply the schema to the dataframe
schemaOrders = spark.read.schema(customerSchema).csv("./customer-orders.csv")

# Register the dataframe as a temporary SQL table
schemaOrders.createOrReplaceTempView("orders")

# Calculate total amount spent by each customer using Spark SQL
totalAmountSpentByCustomer = spark.sql("""
    SELECT customerID, SUM(amountSpent) AS totalAmountSpent
    FROM orders
    GROUP BY customerID
    ORDER BY totalAmountSpent DESC
""")

# Show the results
totalAmountSpentByCustomer.show()

# Stop the Spark session
spark.stop()
