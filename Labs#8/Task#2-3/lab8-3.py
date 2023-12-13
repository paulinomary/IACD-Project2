# Task 2 - Obtain the Word Frequency in a Book
# Spark SQL

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower

spark = SparkSession.builder.appName("WordFrequency").getOrCreate()


file_path = "./Book"
lines = spark.read.option("header", "false").text(file_path)

words = lines.select(explode(split(lower("value"), " ")).alias("word"))
words.createOrReplaceTempView("word_table")

wordCounts = spark.sql("""
    SELECT word, COUNT(*) as count
    FROM word_table
    GROUP BY word
    ORDER BY count DESC
""")


wordCounts.show()

spark.stop()
