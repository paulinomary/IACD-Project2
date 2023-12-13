# Task 1 - Minimum Temperature per Capital
# Spark SQL

# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("lab8-1").getOrCreate()

file = "./1800.csv"

lines = spark.read.csv(file).select("_c0", "_c1", "_c2", "_c3")

weatherSchema = StructType([
    StructField("stationID", StringType()),
    StructField("date", IntegerType()),
    StructField("entryType", StringType()),
    StructField("temperature", FloatType())
])

lines = spark.read.csv(file, schema=weatherSchema)

min_weather = lines.select("stationID", "entryType", "temperature")

min_weather = min_weather.filter(min_weather.entryType == "TMIN")

min_weather = min_weather.select("stationID", "temperature")
min_weather = min_weather.groupBy("stationID").min("temperature").show()

spark.stop()