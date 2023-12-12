# Task 1 - Minimum Temperature per Capital
# Spark SQL

# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("lab8-1").getOrCreate()

file = "./1800.csv"

# Consider only the 4 first columns
lines = spark.read.csv(file).select("_c0", "_c1", "_c2", "_c3")

# Create a Schema for the file
weatherSchema = StructType([
    StructField("stationID", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("entryType", StringType(), True),
    StructField("temperature", FloatType(), True)
])

# Apply the schema to the DataFrame
lines = spark.read.csv(file, schema=weatherSchema)

# Select only the stationID, entryType and temperature columns
min_weather = lines.select("stationID", "entryType", "temperature")

# Filter out all but TMIN entries
min_weather = min_weather.filter(min_weather.entryType == "TMIN")

# Select only the stationID and temperature columns
min_weather = min_weather.select("stationID", "temperature")

# Find minimum temperature per station
min_weather = min_weather.groupBy("stationID").min("temperature").show()

# Close the Spark session
spark.stop()