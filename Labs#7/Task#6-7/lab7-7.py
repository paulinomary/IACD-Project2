from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col


spark = SparkSession.builder.appName("HeroAppearanceCount").getOrCreate()


movies_rdd = spark.read.text("Marvel+Graph").rdd.flatMap(lambda x: x)
names_rdd = spark.read.text("Marvel+Names").rdd.flatMap(lambda x: x)

countEntries = movies_rdd \
    .flatMap(lambda line: line.split(' ')) \
    .filter(lambda hero_id: hero_id != '') \
    .map(lambda hero_id: (hero_id, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

nameMap = names_rdd \
    .map(lambda line: line.split(' ', 1)) \
    .map(lambda parts: (parts[0], parts[1]))

id_least = countEntries.sortBy(lambda x: x[1], ascending=True).first()[0]
name_least = nameMap.filter(lambda x: x[0] == id_least).first()[1]

print(f"\n\nLeast Popular: {name_least}\n\n")

spark.stop()