import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("lab9-1").getOrCreate()

# Read the data from the terminal in a socket
# Create Spark Session in a ReadStream format and to read data from the socket
host = "localhost"
port = 9999

# Establish the connection
lines = spark.readStream.format("socket").option("host", host).option("port", port).load()

# Split the lines into words
words = lines.select(
    pyspark.sql.functions.explode(
        pyspark.sql.functions.split(lines.value, " ")
    ).alias("word")
)

# Create wordCounts variable that groups the words and counts the occurrences
wordCounts = words.groupBy("word").count()

# Order the words by count
wordCounts = wordCounts.orderBy(func.col("count").desc())

# Start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()