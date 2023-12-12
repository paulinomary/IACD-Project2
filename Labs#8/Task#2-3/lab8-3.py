# Task 2 - Obtain the Word Frequency in a Book
# Spark SQL

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower

# Create a Spark session
spark = SparkSession.builder.appName("WordFrequency").getOrCreate()

# Read the lines of the book and save the words
file_path = "./Book"
lines = spark.read.option("header", "false").text(file_path)

# Separate words, convert to lowercase, and explode into individual rows
words = lines.select(explode(split(lower("value"), " ")).alias("word"))

# Register as a temporary table
words.createOrReplaceTempView("word_table")

# Obtain the word frequency using Spark SQL
wordCounts = spark.sql("""
    SELECT word, COUNT(*) as count
    FROM word_table
    GROUP BY word
    ORDER BY count DESC
""")

# Show the results
wordCounts.show()

# Stop the Spark session
spark.stop()
