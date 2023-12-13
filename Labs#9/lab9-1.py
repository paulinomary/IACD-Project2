import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("lab9-1").getOrCreate()

host = "localhost"
port = 9999

lines = spark.readStream.format("socket").option("host", host).option("port", port).load()


words = lines.select(
    pyspark.sql.functions.explode(
        pyspark.sql.functions.split(lines.value, " ")
    ).alias("word")
)

wordCounts = words.groupBy("word").count()

wordCounts = wordCounts.orderBy(func.col("count").desc())

query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()