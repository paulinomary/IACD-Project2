from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Create a Spark session
spark = SparkSession.builder.appName("HeroAppearanceCount").getOrCreate()

# Read data into RDDs
movies_rdd = spark.read.text("Marvel+Graph").rdd.flatMap(lambda x: x)
names_rdd = spark.read.text("Marvel+Names").rdd.flatMap(lambda x: x)

# Process the movies RDD to count hero appearances
movies_hero_count_rdd = movies_rdd \
    .flatMap(lambda line: line.split(' ')) \
    .filter(lambda hero_id: hero_id != '') \
    .map(lambda hero_id: (hero_id, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

# Process the names RDD to create a mapping of hero IDs to names
names_mapping_rdd = names_rdd \
    .map(lambda line: line.split(' ', 1)) \
    .map(lambda parts: (parts[0], parts[1]))

# Find the hero that appears the least
least_appeared_hero_id = movies_hero_count_rdd.sortBy(lambda x: x[1], ascending=True).first()[0]
least_appeared_hero_name = names_mapping_rdd.filter(lambda x: x[0] == least_appeared_hero_id).first()[1]

# Print the result
print(f"\n\nThe hero that appears the least is: {least_appeared_hero_name}\n\n")

# Stop the Spark session
spark.stop()